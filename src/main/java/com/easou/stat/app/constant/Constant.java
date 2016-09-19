package com.easou.stat.app.constant;

import org.nutz.json.JsonFormat;

/**
 * @ClassName: Constant
 * @Description: 客户端常量类
 * @author 张羽强
 * @date 2012-9-3 下午3:52:22
 * 
 */
public interface Constant {

	// 普通0、1的标志位。
	public static String			ZERO						= "0";
	public static String			ONE							= "1";

	/** @Fields ALLAPP : 表示所有客户端 **/
	public static final String		ALLAPP						= "所有";
	public static final String		OTHER_APP					= "其它";
	public static final String		OTHER_ZONE					= "其它";
	public static final String		OTHER_CPID					= "其它";
	public static final String		OTHER_VERSION				= "其它";
	public static final String		OTHER_MODEL					= "其它";
	public static final String		OTHER_APN					= "其它";
	public static final String		OTHER_RESOLUTION			= "其它";
	public static final String		OTHER_OS					= "其它";
	public static final String		OTHER_OS_VERSION			= "其它";
	public static final String		OTHER_NETTYPE				= "其它";

	/** @Fields DELIMITER : 常用的分割标志 **/
	public static final String		DELIMITER					= "{]";
	/** @Fields DELIMITER_REG : 用来切割分割标志的正则表达式串 **/
	public static final String		DELIMITER_REG				= "\\{]";

	/** @Fields ALL : // 标识所有的，每项符合这个键的 **/
	public static final String		ALL							= "*";

	// 常量值
	public static final String		LOGTYPES					= "logTypes";
	public static final String		GRANULARITY					= "granularity";
	public static final String		DATETIME					= "dateTime";
	/** @Fields ANALYSIS_ATTR : 用户自定义属性 **/
	public static final String		ANALYSIS_ATTR				= "analysisAttr";
	/** @Fields DEFINED_EVENT_ID : 用户自定义事件（事件ID） **/
	public static final String		DEFINED_EVENT_ID			= "eventID";
	/** @Fields EVENT_LIST : 用户自定义事件列表 **/
	public static final String		USER_DEFINED_EVENT_LIST		= "eventList";
	/** **/
	public static final String      EVENT_TAKE_RATES_MAP        = "event_take_rates_map";
	/** @Fields phone_brand : 手机品牌 **/
	public static final String		PHONE_BRAND					= "phoneBrand";
	public static final String		_DATETIME					= "_dateTime";
	public static final String		CONFIG_FILE_NAME			= "config.xml";
	public static final String		JOBID						= "jobId";
	public static final String		USER_DEFINED_EVENT			= "userDefinedEvent";
	public static final String		APP_TMP_MAPREDUCE			= "appTmpMapReduce";
	public static final String		APP_TMP_PATH				= "appTmpPath";
	public static final String		APP_MOBILEINFO_MAPREDUCE	= "appMobileInfoMapReduce";
	public static final String		APP_MOBILEINFO_PATH			= "/easou/applog/m";

	public static final int			INT_COUNT_ONE				= 1;
	public static final int			INT_COUNT_ZERO				= 0;
	public static final int			INT_COUNT_TWO				= 2;

	public static final JsonFormat	JSON_FORMAT					= new JsonFormat(true).setIgnoreNull(true).setQuoteName(false);
	// 客户端日志key值
	public static final String		OPENUDID					= AppKey.phone_udid.ordinalString();
	public static final String		APPKEY						= AppKey.appkey.ordinalString();
	public static final String		CPID						= AppKey.cpid.ordinalString();
	public static final String		TYPE						= AppKey.type.ordinalString();
	public static final String		TIME						= AppKey.time.ordinalString();
	public static final String		PHONEAPN					= AppKey.phone_apn.ordinalString();
	public static final String		PHONEMAC					= AppKey.phone_mac.ordinalString();
	public static final String		PHONEIMSI					= AppKey.phone_imsi.ordinalString();
	public static final String		PHONEIMEI					= AppKey.phone_imei.ordinalString();
	public static final String		PHONEMODEL					= AppKey.phone_model.ordinalString();
	public static final String		PHONERESOLUTION				= AppKey.phone_resolution.ordinalString();
	public static final String		PHONESOFTNAME				= AppKey.phone_softname.ordinalString();
	public static final String		PHONESOFTVERSION			= AppKey.phone_softversion.ordinalString();
	public static final String		PHONEFIRMWAREVERSION		= AppKey.phone_firmware_version.ordinalString();
	public static final String		BEHAVIOR					= AppKey.behavior.ordinalString();
	public static final String		EXCEPTION					= AppKey.exception.ordinalString();

	// 客户端指标value值
	public static final String		NUMBER_USERS				= Indicators.NUMBER_USERS.ordinalString();
	public static final String		NUMBER_NEW_USERS			= Indicators.NUMBER_NEW_USERS.ordinalString();
	public static final String		NUMBER_ACITVE_USERS			= Indicators.NUMBER_ACITVE_USERS.ordinalString();
	public static final String		NUMBER_RETAINED_USERS		= Indicators.NUMBER_RETAINED_USERS.ordinalString();
	public static final String		TIMES_START					= Indicators.TIMES_START.ordinalString();
	public static final String		TIME_AVERAGE				= Indicators.TIME_AVERAGE.ordinalString();
	public static final String		NUMBER_ASC_USERS			= Indicators.NUMBER_ASC_USERS.ordinalString();
	public static final String		NUMBER_NET_TYPE				= Indicators.NUMBER_NET_TYPE.ordinalString();

	// 日子类型标识
	public static final String		TYPE_MOBILEPHONEINFO		= "type_mobileinfo";
	public static final String		TYPE_BEHAVIORINFO			= "type_behaviorinfo";
	public static final String		TYPE_EXCEPTIONINFO			= "type_exceptioninfo";
	public static final String      TYPE_ACTIVITYINFO           = "type_activityinfo";
	public static final String		TYPE_SEARCHINFO				= "type_searchinfo";
	public static final String		TYPE_EVENTINFO				= "type_eventinfo";
	public static final String		TMP_MOBILEPHONEINFO			= "tmp_mobileinfo";
	
	public static final String		TYPE_SHAREINFO				= "type_shareinfo";
	public static final String		TYPE_SHARECLICKINFO			= "type_shareclickinfo";

	// 时间判断标志
	public static int				CONDITIONTYPE_SAME_DAY		= 0;															// 同一天
	public static int				CONDITIONTYPE_BEFORE_DAY	= 1;															// 前一天
	public static int				CONDITIONTYPE_WEEK			= 2;															// 上周
	public static int				CONDITIONTYPE_MONTH			= 3;

	// 用户自定义事件规则
	/** @Fields rule_1 : 次数/PV **/
	public static final int			rule_1						= 1;
	/** @Fields rule_2 : 用户数/用户量 **/
	public static final int			rule_2						= 2;
	/** @Fields rule_3 : 流量 **/
	public static final int			rule_3						= 3;
	/** @Fields rule_4 : 时长 **/
	public static final int			rule_4						= 4;
	/** @Fields rule_5 : 歌单 **/
	public static final int			rule_5						= 5;

	// Top 标识
	/** @Fields isTop : Top **/
	public static final String		isTop						= "1";
	/** @Fields notTop : 非Top **/
	public static final String		isNotTop					= "0";
	
	public static final String		DISTINCT					= "distinct";
	
	public static final String		COUNT						= "count";
	
	public static final String		SUM							= "sum";
	
	public static final String		AVG							= "avg";
	
	public static final String 		LOGROOTDIR					="/applog/log/";
	
	public static final String		HDFSURI						="hdfs://10.18.16.206:61700";
	
	public static final String		EVENT_CACHE					="hdfs://10.18.16.206:61700/applog/cache/event_rule.dat";
	public static final String		MR_BASEPACKAGE				="com.easou.stat.app.mapreduce.client.";
}
