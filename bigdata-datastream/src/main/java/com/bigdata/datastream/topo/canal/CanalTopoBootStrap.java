package com.bigdata.datastream.topo.canal;

import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.bigdata.datastream.utils.canal.CanalEmbeddedInstance;

/**
 * Canal Topo
 * 
 * @author fangzhu
 * @date 2016-3-1 启动实例:jstorm jar dataflow.jar
 *       com.bigdata.datastream.topo.canal.CanalTopoBootStrap --name CanalTest
 *       --zookeeper 192.168.1.153:2181,192.168.1.154:2181,192.168.1.156:2181
 *       --broker-conf 192.168.1.153:9092,192.168.1.154:9092,192.168.1.156:9092
 *       --canalzk 192.168.1.153:2181,192.168.1.154:2181,192.168.1.156:2181
 *       --destination test --mysql-address 192.168.1.82 --mysql-port 3901
 *       --mysql-salveId 2325 --mysql_username canal --mysql_password
 *       0d4bd0232365bd36
 */
public class CanalTopoBootStrap {
	class MainConfig {
		@Parameter(names = { "-n", "--name" }, description = "The name of toplogy", required = true)
		public String topologyName;

		@Parameter(names = { "-zk", "--zookeeper" }, description = "The zookeeper hosts", required = true)
		public String zkHosts;

		@Parameter(names = { "-bc", "--broker-conf" }, description = "The brokers conf", required = true)
		public String brokerConf;

		@Parameter(names = { "-cz", "--canalzk" }, description = "canal zk host:port", required = true)
		public String canalZk;

		@Parameter(names = { "-d", "--destination" }, description = "canal destination", required = true)
		public String destination;

		@Parameter(names = { "-madd", "--mysql-address" }, description = "mysql address", required = true)
		public String mysql_address;

		@Parameter(names = { "-mport", "--mysql-port" }, description = "mysql port", required = true)
		public int mysql_port;

		@Parameter(names = { "-ms", "--mysql-salveId" }, description = "mysql salveId", required = true)
		public long mysql_slaveId;

		@Parameter(names = { "-muser", "--mysql_username" }, description = "mysql username", required = true)
		public String mysql_username;

		@Parameter(names = { "-mpwd", "--mysql_password" }, description = "mysql password", required = true)
		public String mysql_password;

		@Parameter(names = { "-kp", "--kafka-path" }, description = "The kafka path in zookeeper")
		public String kafkaPath = "/brokers";

		@Parameter(names = { "-w", "--works" }, description = "The number of works")
		public int numOfWorks = 1;

		@Parameter(names = { "-s", "--spouts" }, description = "The number of spouts")
		public int numOfSpouts = 1;

		@Parameter(names = { "-b", "--bolts" }, description = "The number of bolts")
		public int numOfBolts = 5;

	}

	public MainConfig parseConfig(String[] args) {
		MainConfig config = new MainConfig();
		new JCommander(config, args);
		// 默认
		// if (config.topologyName == null) {
		// config.topologyName = config.topic;
		// }

		return config;
	}

	public CanalEmbeddedSpout buildCanalEmbeddedSpout(MainConfig config) {
		CanalEmbeddedInstance canalEmbeddedInstance = new CanalEmbeddedInstance(
				config.destination, config.canalZk, config.mysql_address,
				config.mysql_port, config.mysql_slaveId, config.mysql_username,
				config.mysql_password);
		CanalEmbeddedSpout canalSpout = new CanalEmbeddedSpout()
				.setCanalEmbeddedInstanceAndDestination(canalEmbeddedInstance,
						config.destination);
		return canalSpout;
	}

	// public CanalClientSpout buildCanalClientSpout(MainConfig config) {
	// CanalClientSpout canalSpout = new CanalClientSpout().setCanalConnect(
	// config.canalZk, config.destination);
	// return canalSpout;
	// }

	public CanalKafkaBolt buildCanalKafkaBolt(MainConfig config) {
		CanalKafkaBolt bolt = new CanalKafkaBolt();
		return bolt;
	}

	public StormTopology buildTopology(MainConfig config) {
		String canalSpout = config.topologyName + "-canalSpout";
		String canalKafkaBolt = config.topologyName + "-canalKafkaBolt";
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(canalSpout, buildCanalEmbeddedSpout(config),
				config.numOfSpouts);
		builder.setBolt(canalKafkaBolt, buildCanalKafkaBolt(config),
				config.numOfBolts).shuffleGrouping(canalSpout);

		return builder.createTopology();

	}

	public static void main(String[] args) throws Exception {
		MainConfig mainConfig = new CanalTopoBootStrap().parseConfig(args);
		System.out.println(mainConfig.toString());

		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 500);
		config.setNumWorkers(mainConfig.numOfWorks);
		config.setNumAckers(0);

		// set kafkabolt properties
		Properties props = new Properties();
		props.put("metadata.broker.list", mainConfig.brokerConf);
		props.put("request.required.acks", "1");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		config.put(CanalKafkaBolt.KAFKA_BROKER_PROPERTIES, props);

		try {
			StormSubmitter.submitTopology(mainConfig.topologyName, config,
					new CanalTopoBootStrap().buildTopology(mainConfig));
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
		System.out.println("start ok...");
	}
}
