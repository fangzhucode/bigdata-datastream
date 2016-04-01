package com.bigdata.test.spring;

import java.util.Properties;

import org.springframework.context.ApplicationContext;

import com.bigdata.datastream.utils.ApplicationContextUtil;


public class TestProperties {
	protected static ApplicationContext appContext=ApplicationContextUtil.getAppContext();
	public static void main(String[] args) {
		Properties props = (Properties) appContext.getBean("kafkaConfig");
		System.out.println(props.get("zookeeper.connect"));
		
	}
}
