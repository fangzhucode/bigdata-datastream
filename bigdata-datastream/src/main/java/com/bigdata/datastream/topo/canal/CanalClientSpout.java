package com.bigdata.datastream.topo.canal;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.bigdata.datastream.conf.CanalClientConf;
import com.bigdata.datastream.entry.CanalRecord;
import com.bigdata.datastream.utils.canal.AnalysisBinLog;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 解析canal binlog
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class CanalClientSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory
			.getLogger(CanalClientSpout.class);
	public String canalZk;
	public String destination;
	private CanalConnector connector = null;
	private long sleepTime = 1000 * 3;
	private SpoutOutputCollector collector;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.connector = CanalConnectors.newClusterConnector(canalZk,
				destination, "", "");
		connector.connect();
		connector.subscribe();

		this.collector = collector;
	}

	public void nextTuple() {
		long batchId = 0;
		try {
			if (!connector.checkValid()) {
				connector.connect();
				connector.subscribe();
			}
			Message message = connector
					.getWithoutAck(CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
			batchId = message.getId();
			int size = message.getEntries().size();
			if (batchId == -1 || size == 0) {
				logger.info("message size=" + size + " batchid=" + batchId
						+ " sleep " + sleepTime + "ms");
				Thread.sleep(sleepTime);
			} else {
				List<CanalRecord> results = AnalysisBinLog
						.analysisBinLog(message.getEntries());
				if (results.size() > 0) {
					// 按条拆分结果 便于后面处理 如反序列化等
					for (CanalRecord canalRecord : results) {
						String topic = canalRecord.getDatabase() + "."
								+ canalRecord.getTable();
						collector.emit(new Values(topic, JSONObject
								.toJSONString(canalRecord)));
						logger.info("result:"
								+ JSONObject.toJSONString(canalRecord));
					}
				} else {
					Thread.sleep(sleepTime);
					logger.info("result size is 0 sleep " + sleepTime);
				}
			}
			connector.ack(batchId); // 提交确认
		} catch (Exception e) {
			connector.rollback(batchId); // 处理失败, 回滚数据
			logger.error("process error! rollback ", e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "message"));
	}

	public CanalClientSpout setCanalConnect(String canalZk, String destination) {
		this.canalZk = canalZk;
		this.destination = destination;
		return this;
	}
}
