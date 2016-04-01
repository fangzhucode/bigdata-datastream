package com.bigdata.datastream.topo.base;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.bigdata.datastream.utils.ApplicationContextUtil;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.MultiScheme;

/**
 * topo kafka spout基本类  通过装饰器实现方式
 * @author fangzhu
 * @date 2016-3-18
 */
public abstract class AbstractKafkaSpout {

	public Logger logger = LoggerFactory.getLogger(getClass());

	private KafkaSpout kafkaSpout;

	public abstract String getTopic();

	public abstract String getSpringConfigFile();

	public abstract String getKafkaComsumerGroupName();

	public abstract MultiScheme getScheme();

	public KafkaSpout getKafkaSpout() {
		if (kafkaSpout == null) {
			String springFile = getSpringConfigFile();
			ApplicationContext ctx = ApplicationContextUtil
					.getAppContext(springFile);
			Properties kafkaConfig = (Properties) ctx.getBean("kafkaConfig");
			String broker = kafkaConfig.getProperty("zookeeper.connect");
			BrokerHosts brokerHosts = new ZkHosts(broker);
			SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, getTopic(),
					"/zkkafkaspout", getKafkaComsumerGroupName());
			spoutConfig.ignoreZkOffsets = true;
			spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
			spoutConfig.metricsTimeBucketSizeInSecs = 120;
			spoutConfig.stateUpdateIntervalMs = 10000;
			spoutConfig.scheme = getScheme();
			kafkaSpout = new KafkaSpout(spoutConfig);
		}
		return kafkaSpout;
	}
}
