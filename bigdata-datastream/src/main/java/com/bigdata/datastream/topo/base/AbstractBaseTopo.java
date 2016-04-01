package com.bigdata.datastream.topo.base;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

/**
 * topo基础类
 * 
 * @author fangzhu
 * @date 2016-3-18
 */
public abstract class AbstractBaseTopo {
	public Logger logger = LoggerFactory.getLogger(getClass());

	public abstract StormTopology buildTopo(Map<String, String> params);

	public String getTopoName(Map<String, String> params) {
		String topoName = params.get("name");
		if (StringUtils.isEmpty(topoName)) {
			return this.getClass().getSimpleName();
		} else {
			return topoName;
		}
	}

	public abstract Config getConfig(Map<String, String> params);

	public abstract void checkParams(Map<String, String> params);
}
