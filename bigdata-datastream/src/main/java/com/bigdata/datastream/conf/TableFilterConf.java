package com.bigdata.datastream.conf;

import com.alibaba.fastjson.JSONObject;

/**
 * 表过滤相关
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class TableFilterConf {

	/**
	 * 是否需要 过滤表的某些字段
	 */
	public static boolean IS_FIELD_INCLUDE = false;

	/**
	 * 需要过滤字段的表
	 */
	public static String FIELD_INCLUDE_TABLES;

	/**
	 * 记录每个需要过滤字段的表的数据结构 key: table_name value: field1,field2..
	 */
	public static JSONObject FIELD_INCLUDE_TABLES_JSON;

	/**
	 * 是否需要 排除表的某些字段
	 */
	public static boolean IS_FIELD_EXCLUDE = false;

	/**
	 * 需要排除字段的表
	 */
	public static String FIELD_EXCLUDE_TABLES;

	/**
	 * 记录每个需要排除字段的表的数据结构 key: table_name value: field1,field2..
	 */
	public static JSONObject FIELD_EXCLUDE_TABLES_JSON;

}
