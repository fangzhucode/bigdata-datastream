package com.bigdata.datastream.entry;

import java.io.Serializable;
import java.util.HashMap;

/**
 * CanalRecord Entry类
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class CanalRecord implements Serializable {
	private static final long serialVersionUID = 1L;
	// 数据库
	private String database;
	// 表
	private String table;
	// DML操作类型
	private String dmlType;
	// 执行时间
	private long executeTime;
	// 操作完结果
	private HashMap<String, String> afterColumn;
	// 操作前结果 update只存有过更改的字段 delete存删除之前数据 insert为null
	private HashMap<String, String> beforeColumn;
	// 更改过的字段
	private String updateFields;

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getDmlType() {
		return dmlType;
	}

	public void setDmlType(String dmlType) {
		this.dmlType = dmlType;
	}

	public long getExecuteTime() {
		return executeTime;
	}

	public void setExecuteTime(long executeTime) {
		this.executeTime = executeTime;
	}

	public HashMap<String, String> getAfterColumn() {
		return afterColumn;
	}

	public void setAfterColumn(HashMap<String, String> afterColumn) {
		this.afterColumn = afterColumn;
	}

	public HashMap<String, String> getBeforeColumn() {
		return beforeColumn;
	}

	public void setBeforeColumn(HashMap<String, String> beforeColumn) {
		this.beforeColumn = beforeColumn;
	}

	public String getUpdateFields() {
		return updateFields;
	}

	public void setUpdateFields(String updateFields) {
		this.updateFields = updateFields;
	}

}
