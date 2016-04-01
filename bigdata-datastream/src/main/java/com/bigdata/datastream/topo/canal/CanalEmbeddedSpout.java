package com.bigdata.datastream.topo.canal;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.bigdata.datastream.conf.CanalClientConf;
import com.bigdata.datastream.entry.CanalRecord;
import com.bigdata.datastream.utils.canal.AnalysisBinLog;
import com.bigdata.datastream.utils.canal.CanalEmbeddedInstance;
import com.bigdata.datastream.utils.canal.CanalServerWithEmbeddedDataStream;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * canal内嵌模式
 * 
 * @author fangzhu
 * @date 2016-3-22
 */
public class CanalEmbeddedSpout extends BaseRichSpout {

	private static final long serialVersionUID = -6399126897739248672L;
	private final static Logger logger = LoggerFactory
			.getLogger(CanalEmbeddedSpout.class);
	private CanalServerWithEmbeddedDataStream server;
	private ClientIdentity clientIdentity;
	private SpoutOutputCollector collector;
	private CanalEmbeddedInstance canalEmbeddedInstance;
	private String canalDestination;
	private static long sleepTime = 1000 * 3;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		logger.info("=====start init canal server=====");
		this.collector = collector;
		clientIdentity = new ClientIdentity(canalDestination, (short) 1);
		server = CanalServerWithEmbeddedDataStream.instance();
		server.setCanalInstanceGenerator(new CanalInstanceGenerator() {
			public CanalInstance generate(String destination) {
				Canal canal = canalEmbeddedInstance.getCanal();
				return new CanalInstanceWithManager(canal,
						CanalClientConf.FILTER);
			}
		});
		server.start();
		server.start(canalDestination);
		logger.info("=====end init canal server=====");
	}

	public void nextTuple() {
		logger.info("start CanalEmbeddedSpout");
		if (!server.isStart()) {
			server.start();
		}
		if (!server.isStart(canalDestination)) {
			server.start(canalDestination);
		}
		server.subscribe(clientIdentity);
		Message message = server
				.get(clientIdentity, CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
		// Message message = server.getWithoutAck(clientIdentity,
		// CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
		long batchId = message.getId();
		int size = message.getEntries().size();
		if (batchId == -1 || size == 0) {
			logger.info("message size=" + size + " batchid=" + batchId
					+ " sleep " + sleepTime + "ms");
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			List<CanalRecord> results = AnalysisBinLog.analysisBinLog(message
					.getEntries());
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
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				logger.info("result size is 0 sleep " + sleepTime);
			}
		}
		// server.ack(clientIdentity, message.getId());
		server.unsubscribe(clientIdentity);
		logger.info("end CanalEmbeddedSpout");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "message"));
	}

	public CanalEmbeddedSpout setCanalEmbeddedInstanceAndDestination(
			CanalEmbeddedInstance canalEmbeddedInstance, String canalDestination) {
		this.canalEmbeddedInstance = canalEmbeddedInstance;
		this.canalDestination = canalDestination;
		return this;
	}
}
