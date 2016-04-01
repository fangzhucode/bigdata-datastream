package com.bigdata.datastream.utils.canal;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;

import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.HAMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.IndexMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.MetaMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.SourcingType;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.StorageMode;

/**
 * CanalInstance实例
 * 
 * @author fangzhu
 * @date 2016-3-22
 */
public class CanalEmbeddedInstance implements Serializable {
	private static final long serialVersionUID = 1L;
	// canalId
	private long canalId;
	// destination名称
	private String destination;
	// 集群zk
	private String cluster;
	// mysql 地址
	private String mysql_address;
	// mysql 端口
	private int mysql_port;
	// mysql slave id
	private long mysqlSlaveId;
	// mysql 登录用户名
	private String mysql_username;
	// mysql 登录密码
	private String mysql_password;

	public Canal getCanal() {
		Canal canal = new Canal();
		canal.setId(canalId);
		canal.setName(destination);
		canal.setDesc("canal server");

		CanalParameter parameter = new CanalParameter();

		parameter.setZkClusters(Arrays.asList(cluster));
		parameter.setMetaMode(MetaMode.MIXED); // 冷备，可选择混合模式
		parameter.setHaMode(HAMode.HEARTBEAT);
		parameter.setIndexMode(IndexMode.META);// 内存版store，需要选择meta做为index

		parameter.setStorageMode(StorageMode.MEMORY);
		parameter.setMemoryStorageBufferSize(32 * 1024);
		parameter.setSourcingType(SourcingType.MYSQL);
		parameter.setDbAddresses(Arrays.asList(new InetSocketAddress(
				mysql_address, mysql_port)));
		parameter.setDbUsername(mysql_username);
		parameter.setDbPassword(mysql_password);
		parameter.setSlaveId(mysqlSlaveId);
		// parameter
		// .setPositions(Arrays
		// .asList("{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}",
		// "{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}"));

		parameter.setDefaultConnectionTimeoutInSeconds(30);
		parameter.setConnectionCharset("UTF-8");
		parameter.setConnectionCharsetNumber((byte) 33);
		parameter.setReceiveBufferSize(8 * 1024);
		parameter.setSendBufferSize(8 * 1024);
		parameter.setDetectingEnable(false);
		parameter.setDetectingIntervalInSeconds(10);
		parameter.setDetectingRetryTimes(3);
		// parameter.setDetectingSQL(DETECTING_SQL);

		canal.setCanalParameter(parameter);
		return canal;
	}

	public CanalEmbeddedInstance() {
	}

	public CanalEmbeddedInstance(String destination, String cluster,
			String mysql_address, int mysql_port, long mysqlSlaveId,
			String mysql_username, String mysql_password) {
		this.destination = destination;
		this.cluster = cluster;
		this.mysql_address = mysql_address;
		this.mysql_port = mysql_port;
		this.mysqlSlaveId = mysqlSlaveId;
		this.mysql_username = mysql_username;
		this.mysql_password = mysql_password;
	}

}
