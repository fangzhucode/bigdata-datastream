package com.bigdata.datastream.utils.canal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.bigdata.datastream.conf.CanalClientConf;
import com.bigdata.datastream.entry.CanalRecord;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 解析mysql binlog日志
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class AnalysisBinLog {
	private static Logger log = LoggerFactory.getLogger(AnalysisBinLog.class);

	public static List<CanalRecord> analysisBinLog(List<Entry> entrys) {
		List<CanalRecord> canalRecords = new ArrayList<CanalRecord>();
		String sql = "";
		for (Entry entry : entrys) {
			if (entry.getEntryType() == EntryType.ROWDATA) {
				RowChange rowChage = null;
				try {
					rowChage = RowChange.parseFrom(entry.getStoreValue());
				} catch (InvalidProtocolBufferException e1) {
					log.error("parse from storevalue to rowchage InvalidProtocolBufferException");
					continue;
				}
				EventType eventType = rowChage.getEventType();
				sql = rowChage.getSql() + SystemUtils.LINE_SEPARATOR;

				// 获取参数
				Header header = entry.getHeader();
				String tableName = header.getTableName();
				String database = header.getSchemaName();
				long executeTime = header.getExecuteTime();

				// log.info("--T E S T--");
				// log.info("Entry: " + entry.toString());
				// log.info("EventType: " + eventType);
				// log.info("DDL: " + rowChage.getIsDdl());
				// log.info("Row Change: " + rowChage.toString());

				// 过滤非DML操作
				if (eventType != EventType.INSERT
						&& eventType != EventType.DELETE
						&& eventType != EventType.UPDATE) {
					log.info("ignore query or ddl:###" + eventType
							+ " binlog executeTime:" + executeTime);
					log.info("sql##:" + sql);
					continue;
				}

				// // 过滤查询和DDL操作
				// if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
				// log.info("ignore query or ddl :"+database+"."+tableName+" binlog executeTime:"+executeTime);
				// continue;
				// }

				// 过滤没有解析
				if (StringUtils.isEmpty(database)
						|| StringUtils.isEmpty(tableName)) {
					log.info("database or table is null parse error");
					continue;
				}
				// 过滤database table
				if (!CanalClientConf.INCLUDE_DATABASE.contains(database)
						|| !CanalClientConf.INCLUDE_TABLES.contains(tableName)) {
					log.info("ignore database.table : " + database + "."
							+ tableName + " binlog executeTime:" + executeTime);
					continue;
				}

				try {
					for (RowData rowData : rowChage.getRowDatasList()) {
						if (eventType == EventType.DELETE) {
							canalRecords.add(handerInsertAndDeleteColumn(
									database, tableName, "delete", executeTime,
									rowData.getBeforeColumnsList()));
						} else if (eventType == EventType.INSERT) {
							canalRecords.add(handerInsertAndDeleteColumn(
									database, tableName, "insert", executeTime,
									rowData.getAfterColumnsList()));
						} else if (eventType == EventType.UPDATE) {
							canalRecords.add(handerUpdateColumn(database,
									tableName, "update", executeTime,
									rowData.getBeforeColumnsList(),
									rowData.getAfterColumnsList()));
						} else {
							log.warn("Cannot identify eventtype:" + eventType);
						}
						log.info("success sql:" + sql);
					}
				} catch (Exception e) {
					log.error("fail_sql : " + sql);
					continue;
				}
			}
		}
		return canalRecords;
	}

	/**
	 * 处理mysql insert delete
	 * 
	 * @param database
	 * @param table
	 * @param dmlType
	 * @param executeTime
	 * @param columns
	 * @return
	 */
	public static CanalRecord handerInsertAndDeleteColumn(String database,
			String table, String dmlType, long executeTime, List<Column> columns) {
		HashMap<String, String> columnsMap = new HashMap<String, String>();
		for (Column column : columns) {
			String name = column.getName();
			String value = column.getValue();
			columnsMap.put(name, value);
			// log.info("name:" + name + " value:" + value);
		}

		// 封装成CanalRecord
		CanalRecord canalRecord = new CanalRecord();
		canalRecord.setDatabase(database);
		canalRecord.setTable(table);
		canalRecord.setDmlType(dmlType);
		canalRecord.setExecuteTime(executeTime);
		if ("insert".equals(dmlType)) {
			canalRecord.setAfterColumn(columnsMap);
		} else {
			canalRecord.setBeforeColumn(columnsMap);
		}

		return canalRecord;
	}

	/**
	 * 处理mysql update
	 * 
	 * @param database
	 * @param table
	 * @param dmlType
	 * @param executeTime
	 * @param beforeColumns
	 * @param afterColumns
	 * @return
	 */
	public static CanalRecord handerUpdateColumn(String database, String table,
			String dmlType, long executeTime, List<Column> beforeColumns,
			List<Column> afterColumns) {
		// 记录更改过的字段
		StringBuffer updateFields = new StringBuffer();
		// 记录更改后全部数据
		HashMap<String, String> afterColumnsMap = new HashMap<String, String>();
		for (Column column : afterColumns) {
			String name = column.getName();
			String value = column.getValue();
			Boolean isUpdate = column.getUpdated();
			if (isUpdate) {
				if (updateFields.length() == 0) {
					updateFields.append(name);
				} else {
					updateFields.append("," + name);
				}
			}
			afterColumnsMap.put(name, value);
			// log.info("name:" + name + " value:" + value);
		}

		// 记录有过更改字段 变化前的数据
		HashMap<String, String> beforeChangeFiledsMap = new HashMap<String, String>();
		for (Column column : beforeColumns) {
			String name = column.getName();
			if (updateFields.toString().contains(name)) {
				String value = column.getValue();
				beforeChangeFiledsMap.put(name, value);
			}
			// log.info("name:" + name + " value:" + value);
		}

		// 封装成CanalRecord
		CanalRecord canalRecord = new CanalRecord();
		canalRecord.setDatabase(database);
		canalRecord.setTable(table);
		canalRecord.setDmlType(dmlType);
		canalRecord.setExecuteTime(executeTime);
		canalRecord.setUpdateFields(updateFields.toString());
		canalRecord.setAfterColumn(afterColumnsMap);
		canalRecord.setBeforeColumn(beforeChangeFiledsMap);

		return canalRecord;
	}

}
