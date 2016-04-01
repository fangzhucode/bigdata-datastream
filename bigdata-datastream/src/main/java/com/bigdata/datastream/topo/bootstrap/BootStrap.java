package com.bigdata.datastream.topo.bootstrap;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;




import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.datastream.topo.base.AbstractBaseTopo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

/**
 * Topo编程规范 启动示例：
 * jstorm jar redcliff-antman.jar com.dianwoba.bigdata.storm.common.bootstrap.BootStrap  com.dianwoba.redcliff.antman.monitor.topo.LogMonitorTopo [local] k1=v1 k2=v2 k3=v3
 * @author fangzhu
 * @date 2016-3-18
 */
public class BootStrap {
	protected  Logger logger=LoggerFactory.getLogger(getClass());
	
	public static void main(String[] args)throws Exception {
		//args=new String[]{LogMonitorTopo2.class.getName()};
		if (args == null || args.length == 0) {
			System.out.println("启动失败，请指定需要运行的Topology类");
			return;
		}
		
		Map<String,String> argMap=parseConfig(args);
		Class<?> clazz=null;
		try {
			clazz = Class.forName(argMap.get("topoClass"));
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			System.out.println("启动失败，类路径中找不到指定的Topology类");
			System.exit(0);
		}
	    
	    Type father=clazz.getGenericSuperclass();
	    if(((Class<?>)father).getName().equals(AbstractBaseTopo.class.getName())){
	    	AbstractBaseTopo t;
			try {
				t = (AbstractBaseTopo)clazz.newInstance();
				StormTopology topo=t.buildTopo(argMap);
		    	String name=t.getTopoName(argMap);
		    	Config conf=t.getConfig(argMap);
		    	submit(conf,topo,name);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
	    }else{
	    	throw new Exception("使用Bootstrap启动拓扑，需要遵循Topo编程规范！");
	    }

	}


	
	public static void submit(Config conf,StormTopology topo,String name) throws AlreadyAliveException, InvalidTopologyException{
		StormSubmitter.submitTopologyWithProgressBar(name, conf, topo);
	}
	
	public static void localSubmit(Config conf,StormTopology topo,String name) throws InterruptedException{
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(name, conf, topo);
		Thread.sleep(60000);
		cluster.shutdown();
	}
	
	public static Map<String,String> parseConfig(String[] args){
		Map<String,String> map=new HashMap<String,String>();
		map.put("topoClass", args[0]);
		if(args.length>=2){
			if(StringUtils.equals("local", args[1])){
				map.put("local","local");
			}
			
			for(int i=1;i<args.length;i++){
				if(args[i].contains("=")){
					String[] kv=StringUtils.split(args[i],"=");
					if(kv.length==2){
						map.put(kv[0], kv[1]);
					}
				}
			}
		}
		return map;
	}
}
