package com.bigdata.datastream.topo.canal;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.kafka.bolt.selector.KafkaTopicSelector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 写Kafka
 * @author fangzhu
 * @date 2016-3-1
 */
public class CanalKafkaBolt<K, V> extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(CanalKafkaBolt.class);

	public static final String TOPIC = "topic";
	public static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";

	private Producer<K, V> producer;
	private OutputCollector collector;
	private TupleToKafkaMapper<K, V> mapper;
	private KafkaTopicSelector topicSelector;
	private Map<String, String> logTopicMapper = new HashMap<String, String>();

	public CanalKafkaBolt<K, V> withTupleToKafkaMapper(
			TupleToKafkaMapper<K, V> mapper) {
		this.mapper = mapper;
		return this;
	}

	public CanalKafkaBolt<K, V> withTopicSelector(KafkaTopicSelector selector) {
		this.topicSelector = selector;
		return this;
	}

	public CanalKafkaBolt withConfigLogTopic(Map<String, String> map) {
		this.logTopicMapper = map;
		return this;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// for backward compatibility.
		if (mapper == null) {
			this.mapper = new FieldNameBasedTupleToKafkaMapper<K, V>();
		}

		// for backward compatibility.
		// if (topicSelector == null) {
		// this.topicSelector = new DefaultTopicSelector(
		// (String) stormConf.get(TOPIC));
		// }

		Map configMap = (Map) stormConf.get(KAFKA_BROKER_PROPERTIES);
		Properties properties = new Properties();
		properties.putAll(configMap);
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<K, V>(config);
		this.collector = collector;
	}

	public void execute(Tuple input) {
		LOG.info("start CanalKafkaBolt");
		// if (TupleUtils.isTick(input)) {
		// collector.ack(input);
		// return; // Do not try to send ticks to Kafka
		// }

		K key = null;
		V message = null;
		String topic = null;
		try {
			topic = (String) mapper.getKeyFromTuple(input);
			message = mapper.getMessageFromTuple(input);
			if (topic != null) {
				LOG.info("send msg to topic:" + topic + " msg:" + message);
				producer.send(new KeyedMessage<K, V>(topic, null, message));
			} else {
				LOG.warn("skipping topic = " + topic
						+ ", topic selector returned null.");
			}
			collector.ack(input);
		} catch (Exception ex) {
			collector.reportError(ex);
			collector.fail(input);
		}
		LOG.info("end CanalKafkaBolt");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
