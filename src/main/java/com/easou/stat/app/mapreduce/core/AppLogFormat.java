package com.easou.stat.app.mapreduce.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nutz.json.Json;

import com.easou.stat.app.constant.AppKey;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.ConstantProvince;
import com.easou.stat.app.constant.LogTypes;
import com.easou.stat.app.util.Apn;
import com.easou.stat.app.util.CommonUtils;

/**
 * @ClassName: AppKey
 * @Description: 客户端日志Bean
 * @author 张羽强
 * @date 2012-9-4 下午4:40:23
 * 
 */
public class AppLogFormat {

	public static final Log LOG_MR = LogFactory.getLog(AppLogFormat.class);
	/** @Fields openudid : 用户唯一标志 **/
	private String openudid;
	/** @Fields appkey : 客户端标识 **/
	private String appkey;
	/** @Fields cpid : 渠道ID **/
	private String cpid;
	private String type;
	/** @Fields time : 激活时间 **/
	private String time;
	/** @Fields phoneCity : 上网地区 **/
	private String phoneCity;
	/** @Fields phoneApn : 运营商（中国移动） **/
	private String phoneApn;
	/** @Fields phoneMac : MAC地址（70:F3:95:BC:EA:C8） **/
	private String phoneMac;
	/** @Fields phoneImsi : 国际移动用户识别码（460005213297986） **/
	private String phoneImsi;
	
	/** esid **/
    private String phoneESID;
	/** @Fields phoneImei : 国际移动设备身份码（352790041126202） **/
	private String phoneImei;
	/** @Fields phoneModel :手机型号（HTC Hero） **/
	private String phoneModel;
	/** @Fields phoneResolution : 分辨率(height*width) **/
	private String phoneResolution;
	/** @Fields phoneSoftname : 软件包名称（com.users.analysis） **/
	private String phoneSoftname;
	
	private String server_time;
	/**
	 * @Fields phoneSoftversion : 软件包版本（客户端版本）（versionName+versionCode 客户端XML里配置
	 *         1.01）
	 **/
	private String phoneSoftversion;
	/** @Fields phoneFirmwareVersion : 客户端系统版本（2.1-update1） **/
	private String phoneFirmwareVersion;
	/** @Fields phoneFirstRun : 是否为第一次运行（1:是;0:否） **/
	private String phoneFirstRun;
	/** @Fields currentnetWorktype : 上网方式（WIFI,其他） **/
	private String currentnetWorktype;
	/** @Fields phoneStart : 启动 **/
	private String phoneStart;
	/** @Fields os : 操作系统 **/
	private String os;
	/** @Fields sdk_version : SDK 版本 **/
	private String sdkVersion;
	/** @Fields traffic : 流量 **/
	private String traffic;
	/** @Fields salechannel : 渠道（1002/2054） **/
	private String salechannel;
	
	private String exceptVersion;

	private LinkedList<String> exception;
	private LinkedList<String> behaviorList;
	private LinkedList<String> activityList;
	private Map<String, Object> mapLog = new HashMap<String, Object>();

	public AppLogFormat(String openudid, String appkey, String server_time, String cpid,
			String type, String time, String phoneCity, String phoneApn,
			String phoneMac, String phoneImsi, String phoneImei,
			String phoneModel, String phoneResolution, String phoneSoftname,
			String phoneSoftversion, String phoneFirmwareVersion,
			String phoneFirstRun, String currentnetWorktype, String phoneStart,
			String os, String sdkVersion, String traffic, String salechannel,
			LinkedList<String> exception, LinkedList<String> behaviorList,
			LinkedList<String> activityList, Map<String, Object> mapLog,
			String phoneESID) {
		super();
		this.openudid = openudid;
		this.appkey = appkey;
		this.cpid = cpid;
		this.type = type;
		this.time = time;
		this.server_time = server_time;
		this.phoneCity = phoneCity;
		this.phoneApn = phoneApn;
		this.phoneMac = phoneMac;
		this.phoneImsi = phoneImsi;
		this.phoneImei = phoneImei;
		this.phoneModel = phoneModel;
		this.phoneResolution = phoneResolution;
		this.phoneSoftname = phoneSoftname;
		this.phoneSoftversion = phoneSoftversion;
		this.phoneFirmwareVersion = phoneFirmwareVersion;
		this.phoneFirstRun = phoneFirstRun;
		this.currentnetWorktype = currentnetWorktype;
		this.phoneStart = phoneStart;
		this.os = os;
		this.sdkVersion = sdkVersion;
		this.traffic = traffic;
		this.salechannel = salechannel;
		this.exception = exception;
		this.behaviorList = behaviorList;
		this.mapLog = mapLog;
		this.activityList = activityList;
		this.phoneESID = phoneESID;
	}

	@SuppressWarnings("unchecked")
	public AppLogFormat(String line) {
		super();
		try {
			Map<String, Object> _map = Json.fromJson(Map.class, line);
			this.openudid = (String) _map.get(AppKey.phone_udid.toString());
			this.appkey = (String) _map.get(AppKey.appkey.toString());
			this.cpid = (String) _map.get(AppKey.cpid.toString());
			this.type = (String) _map.get(AppKey.type.toString());
			this.time = String.valueOf(_map.get(AppKey.time.toString()));
			this.server_time = (String) _map.get(AppKey.server_time.toString());
			this.phoneCity = (String) _map.get(AppKey.phone_city.toString());
			this.phoneApn = (String) _map.get(AppKey.phone_apn.toString());
			this.phoneMac = (String) _map.get(AppKey.phone_mac.toString());
			this.phoneImsi = (String) _map.get(AppKey.phone_imsi.toString());
			this.phoneImei = (String) _map.get(AppKey.phone_imei.toString());
			this.phoneModel = (String) _map.get(AppKey.phone_model.toString());
			this.phoneResolution = (String) _map.get(AppKey.phone_resolution
					.toString());
			this.phoneSoftname = (String) _map.get(AppKey.phone_softname
					.toString());
			this.phoneSoftversion = (String) _map.get(AppKey.phone_softversion
					.toString());
			this.phoneFirmwareVersion = (String) _map
					.get(AppKey.phone_firmware_version.toString());
			this.phoneFirstRun = String.valueOf(_map.get(AppKey.phone_first_run
					.toString()));
			this.currentnetWorktype = (String) _map
					.get(AppKey.currentnetworktype.toString());
			this.phoneStart = (String) _map.get(AppKey.phoneStart.toString());
			this.os = (String) _map.get(AppKey.os.toString());
			this.sdkVersion = (String) _map.get(AppKey.sdk_version.toString());
			this.traffic = String.valueOf(_map.get(AppKey.traffic.toString()));
			this.salechannel = String.valueOf(_map.get(AppKey.salechannel
					.toString()));
			this.exception = (LinkedList<String>) _map.get(AppKey.exception
					.toString());
			this.behaviorList = (LinkedList<String>) _map.get(AppKey.behavior
					.toString());
			this.mapLog = _map;
			this.activityList = (LinkedList<String>)_map.get(AppKey.activities.toString());
			this.phoneESID = (String)_map.get(AppKey.phone_esid.toString());
			this.exceptVersion = (String)_map.get("version");
		} catch (Exception e) {
			System.out.println("错误的日志行:" + line);
			LOG_MR.error("错误的日志行:" + line, e);
		}
	}

	public boolean isMobilePhoneInfo() {
		if (mapLog.containsKey(AppKey.type.toString())
				&& mapLog.get(AppKey.type.toString().toLowerCase()).equals(
						LogTypes.M.getDefaultValue())) {
			return true;
		}
		return false;
	}

	public boolean isBehaviorInfo() {
		if (mapLog.containsKey(AppKey.type.toString())
				&& mapLog.get(AppKey.type.toString().toLowerCase()).equals(
						LogTypes.B.getDefaultValue())) {
			return true;
		}
		return false;
	}

	public boolean isExceptionInfo() {
		if (mapLog.containsKey(AppKey.type.toString())
				&& mapLog.get(AppKey.type.toString().toLowerCase()).equals(
						LogTypes.E.getDefaultValue())) {
			return true;
		}
		return false;
	}

	public boolean isActivityInfo() {
		if (mapLog.containsKey(AppKey.type.toString())
				&& mapLog.get(AppKey.type.toString().toLowerCase()).equals(
						LogTypes.A.getDefaultValue())) {
			return true;
		}
		return false;
	}

	public String getTime() {
		return time;
	}

	public String getStrTime() {
		return CommonUtils.getDateTime(time, "yyyyMMddHHmm");
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getOpenudid() {
		return openudid;
	}

	public void setOpenudid(String openudid) {
		this.openudid = openudid;
	}

	public String getServer_time() {
		return server_time;
	}

	public void setServer_time(String server_time) {
		this.server_time = server_time;
	}

	public String getAppkey() {
		return appkey;
	}

	public void setAppkey(String appkey) {
		this.appkey = appkey;
	}

	public String getPhoneImsi() {
		return phoneImsi;
	}

	public void setPhoneImsi(String phoneImsi) {
		this.phoneImsi = phoneImsi;
	}

	public String getPhoneApn() {
		return Apn.getPhoneApn(phoneApn);
	}

	public void setPhoneApn(String phoneApn) {
		this.phoneApn = phoneApn;
	}

	public String getPhoneSoftversion() {
		return phoneSoftversion;
	}

	public void setPhoneSoftversion(String phoneSoftversion) {
		this.phoneSoftversion = phoneSoftversion;
	}

	public String getPhoneSoftname() {
		return phoneSoftname;
	}

	public void setPhoneSoftname(String phoneSoftname) {
		this.phoneSoftname = phoneSoftname;
	}

	public String getType() {
		return type;
	}
	
	public String getExceptVersion() {
		return exceptVersion;
	}

	public void setExceptVersion(String exceptVersion) {
		this.exceptVersion = exceptVersion;
	}

	public String getLogType() {
		String t = "";
		if (StringUtils.isNotEmpty(type)) {
			if (type.equals(Constant.TYPE_MOBILEPHONEINFO)) {
				return LogTypes.M.toString();
			}
			if (type.equals(Constant.TYPE_BEHAVIORINFO)) {
				return LogTypes.B.toString();
			}
			if (type.equals(Constant.TYPE_EXCEPTIONINFO)) {
				return LogTypes.E.toString();
			}
		}
		return t;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getPhoneImei() {
		return phoneImei;
	}

	public void setPhoneImei(String phoneImei) {
		this.phoneImei = phoneImei;
	}

	public String getPhoneModel() {
		return phoneModel;
	}

	public void setPhoneModel(String phoneModel) {
		this.phoneModel = phoneModel;
	}

	public LinkedList<String> getBehaviorList() {
		return behaviorList;
	}
	
	public LinkedList<String> getActivityList() {
		return activityList;
	}

	public void setActivityList(LinkedList<String> activityList) {
		this.activityList = activityList;
	}
	

	public String getPhoneESID() {
		return phoneESID;
	}

	public void setPhoneESID(String phoneESID) {
		this.phoneESID = phoneESID;
	}

	/**
	 * @Title: getBehaviors
	 * @Description: 获取解析好的用户行为
	 * @return
	 * @author vic
	 * @return List<Behavior>
	 * @throws
	 */
	public List<Behavior> getBehaviors() {
		List<Behavior> behaviors = new ArrayList<Behavior>();
		List<Map> b = Json.fromJsonAsList(Map.class, Json.toJson(behaviorList));
		if (null != b && b.size() > 0) {
			Behavior behavior = null;
			try {
				for (int i = 0; i < b.size(); i++) {
					behavior = new Behavior();
					behavior.setOp(String.valueOf(b.get(i).get("op")));
					behavior.setTime(String.valueOf(b.get(i).get("time")));
					Map<String, Object> info = Json.fromJson(Map.class,
							Json.toJson(b.get(i).get("info")));
					String strData = String.valueOf(info.get("data"));
					if (StringUtils.isNumeric(strData)) {
						int data = Integer.parseInt(strData);
						behavior.setData(data);
						if (data == 0) {
							behavior.setValues(String.valueOf(info
									.get("values")));
						} else if (data == 1) {
							Map<String, String> propertiesValue = Json
									.fromJson(Map.class,
											Json.toJson(info.get("values")));
							behavior.setPropertiesValue(propertiesValue);
						}
					}
					behaviors.add(behavior);
				}
			} catch (Exception e) {
				LOG_MR.error("获取解析好的用户行为出错", e);
			}
		}
		return behaviors;
	}
	
	/**
	 * 获取activity日志
	 * @return
	 */
	public List<Activity> getActivitys() {
		List<Activity> activitys = new ArrayList<Activity>();
		List<Map> b = Json.fromJsonAsList(Map.class, Json.toJson(activityList));
		if (null != b && b.size() > 0) {
			Activity activity = null;
			try {
				for (int i = 0; i < b.size(); i++) {
					activity = new Activity();
					activity.setCurrentActivity(String.valueOf(b.get(i).get("current_activity")));
					activity.setReferActivity(String.valueOf(b.get(i).get("refer_activity")));
					activity.setTime(String.valueOf(b.get(i).get("time")));
					
					activitys.add(activity);
				}
			} catch (Exception e) {
				LOG_MR.error("获取解析好的activity日志出错", e);
			}
		}
		return activitys;
	}
	
	/**
	 * 获取异常日志
	 * @return
	 */
	public List<ExceptionInfo> getExceptions() {
		List<ExceptionInfo> infos = new ArrayList<ExceptionInfo>();
		List<Map> b = Json.fromJsonAsList(Map.class, Json.toJson(this.exception));
		if (null != b && b.size() > 0) {
			ExceptionInfo info = null;
			try {
				for (int i = 0; i < b.size(); i++) {
					info = new ExceptionInfo();
					info.setClassName(String.valueOf(b.get(i).get("className")));
					info.setStack(String.valueOf(b.get(i).get("stack")));
					info.setTime(String.valueOf(b.get(i).get("time")));
					info.setType(String.valueOf(b.get(i).get("type")));
					infos.add(info);
				}
				
			} catch (Exception e) {
				LOG_MR.error("获取解析好的异常信息出错", e);
			}
		}
		
		return infos;
		
	}

	public void setBehaviorList(LinkedList<String> behaviorList) {
		this.behaviorList = behaviorList;
	}

	public String getPhoneFirmwareVersion() {
		return phoneFirmwareVersion;
	}

	/**
	 * @Title: getPhoneFirmwareVersionNormal
	 * @Description: 取出规范的内核版本（过滤非数字字符-小数点当做数字字符看-再截取前五位）
	 * @return
	 * @author 邓长春
	 * @return String
	 * @throws
	 */
	public String getPhoneFirmwareVersionNormal() {
		String temp = CommonUtils.filterNotNumberChar(phoneFirmwareVersion);
		temp = CommonUtils.truncate(temp, '.', 1);
		if (temp.length() > 5) {
			temp = temp.substring(0, 5);
		}
		temp = CommonUtils.filterCharOfHeadAndTailWithChar(temp, '.');
		if (CommonUtils.isAllNumber(temp) || CommonUtils.isAllChar(temp, '.')) {
			return Constant.OTHER_OS_VERSION;
		} else {
			return temp;
		}
	}

	public void setPhoneFirmwareVersion(String phoneFirmwareVersion) {
		this.phoneFirmwareVersion = phoneFirmwareVersion;
	}

	public Map<String, Object> getMapLog() {
		return mapLog;
	}

	public void setMapLog(Map<String, Object> mapLog) {
		this.mapLog = mapLog;
	}

	public String getPhoneMac() {
		return phoneMac;
	}

	public void setPhoneMac(String phoneMac) {
		this.phoneMac = phoneMac;
	}

	public String getCpid() {
		return cpid;
	}

	public void setCpid(String cpid) {
		this.cpid = cpid;
	}

	public String getPhoneResolution() {
		return phoneResolution;
	}

	public void setPhoneResolution(String phoneResolution) {
		this.phoneResolution = phoneResolution;
	}

	public String getPhoneCity() {
		return phoneCity;
	}

	public void setPhoneCity(String phoneCity) {
		this.phoneCity = phoneCity;
	}

	public String getPhoneFirstRun() {
		return phoneFirstRun;
	}

	public void setPhoneFirstRun(String phoneFirstRun) {
		this.phoneFirstRun = phoneFirstRun;
	}

	public String getCurrentnetWorktype() {
		return currentnetWorktype;
	}

	public void setCurrentnetWorktype(String currentnetWorktype) {
		this.currentnetWorktype = currentnetWorktype;
	}

	public String getPhoneStart() {
		return phoneStart;
	}

	public void setPhoneStart(String phoneStart) {
		this.phoneStart = phoneStart;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getSdkVersion() {
		return sdkVersion;
	}

	public void setSdkVersion(String sdkVersion) {
		this.sdkVersion = sdkVersion;
	}

	public String getSalechannel() {
		return salechannel;
	}

	public void setSalechannel(String salechannel) {
		this.salechannel = salechannel;
	}

	public String getTraffic() {
		return traffic;
	}

	public void setTraffic(String traffic) {
		this.traffic = traffic;
	}

	public void setException(LinkedList<String> exception) {
		this.exception = exception;
	}

	public LinkedList<String> getException() {
		return exception;
	}

	/**
	 * @Title: getProvince
	 * @Description: 
	 *               取出地区(phoneCity)中的省份信息(不包括“省,自治区,直辖市”等一类的标识字)。当前的phoneCity格式为
	 *               ："中国,广东省,深圳市,南山区".
	 * @return
	 * @author 邓长春
	 * @return String
	 * @throws
	 */
	public String getProvince() {
		String province = "";
		String[] tmp = getPhoneCity().split(",");
		try {
			if (tmp.length >= 2) {
				if (tmp[1].indexOf("黑龙江") != -1) {
					province = tmp[1].substring(0, 3);
				} else if (tmp[1].length() >= 2) {
					province = tmp[1].substring(0, 2);
				} else {
					province = "其他";
				}
			} else {
				province = "其它";
			}
		} catch (Exception e) {
			System.out.println(e.toString() + ":" + getPhoneCity());
			LOG_MR.error("解析省份数据出错,日志中省为：" + getPhoneCity(), e);
			return "其他";
		}
		if (ConstantProvince.provinceList.contains(province)) {
			return province;
		} else {
			return "其它";
		}

	}

	/**
	 * @Title: getSalechannel1All
	 * @Description: 获取第一级频道所有(199900000000)
	 * @return
	 * @author vic
	 * @return String
	 * @throws
	 */
	public String getSalechannel1All() {
		return "199900000000";
	}

	/**
	 * @Title: getSalechannel1
	 * @Description: 获取第一级频道(199800000000)
	 * @return
	 * @author vic
	 * @return String
	 * @throws
	 */
	public String getSalechannel1() {
		String salechannel1 = "199800000000";
		if (StringUtils.isNotBlank(getSalechannel())
				&& getSalechannel().indexOf("/") > 0) {
			salechannel1 = getSalechannel().split("/")[0] + "00000000";
		}
		return salechannel1;
	}

	/**
	 * @Title: getSalechannel2
	 * @Description: 获取第二级频道(199829980000)
	 * @return
	 * @author vic
	 * @return String
	 * @throws
	 */
	public String getSalechannel2() {
		String salechannel2 = "199829980000";
		if (StringUtils.isNotBlank(getSalechannel())
				&& getSalechannel().indexOf("/") > 0) {
			salechannel2 = getSalechannel().split("/")[0]
					+ getSalechannel().split("/")[1] + "0000";
		}
		return salechannel2;
	}

	public static void main(String[] args) {
		String m = "{\"os\":\"android\",\"phone_first_run\":1,\"phone_resolution\":\"960*540\",\"phone_softversion\":\"1.0.0.33\",\"appkey\":\"easou88\",\"currentnetworktype\":\"wifi\",\"cpid\":\"\",\"versioncode\":\"3\",\"phone_softname\":\"com.easou.music\",\"type\":\"type_mobileinfo\",\"phone_imei\":\"358862040108194\",\"phone_apn\":\"\",\"phone_udid\":\"46E7164BA2D10F9223E43F45E4500552\",\"time\":0,\"phone_mac\":\"00:16:D4:A5:DB:B1\",\"phone_firmware_version\":\"2.3.5\",\"phone_city\":\"\",\"phone_model\":\"Dell V04B\",\"phone_udid\":\"A7163581682B8F058441D094311CC9E7\",\"phone_first_run\":1,\"os\":\"android\",\"time\":1348213451629,\"phone_mac\":\"04:46:65:F1:A7:28\",\"behavior\":[{\"op\":\"0-MainActivity.onCreate\",\"time\":\"1348213451785\",\"info\":{\"data\":1,\"values\":{\"001\":\"value001\",\"002\":\"value002\"}}},{\"op\":\"0-MainActivity.onCreate\",\"time\":\"1348213451785\",\"info\":{\"data\":0,\"values\":\"启动\"}}],\"appkey\":\"abcdaef\",\"phone_esid\":\"8LLaHt3O8Mh\",\"phone_imsi\":\"460013000629662\",\"type\":\"type_behaviorinfo\",\"phone_city\":\"中国,广东省,深圳市,福田区\",\"phone_imei\":\"357947041418121\"}";
		AppLogFormat _l = new AppLogFormat(m);

		System.out.println("getOpenudid:  " + _l.getOpenudid());
		System.out.println("getAppkey:  " + _l.getAppkey());
		System.out.println("getCpid:  " + _l.getCpid());
		System.out.println("getType:  " + _l.getType());
		System.out.println("getTime:  " + _l.getTime());
		System.out.println("getPhoneApn:  " + _l.getPhoneApn());
		System.out.println("getPhoneMac:  " + _l.getPhoneMac());
		System.out.println("getPhoneImsi:  " + _l.getPhoneImsi());
		System.out.println("getPhoneImei:  " + _l.getPhoneImei());
		System.out.println("getPhoneModel:  " + _l.getPhoneModel());
		System.out.println("getPhoneCcreen:  " + _l.getPhoneResolution());
		System.out.println("getPhoneSoftname:  " + _l.getPhoneSoftname());
		System.out.println("getPhoneSoftversion:  " + _l.getPhoneSoftversion());
		System.out.println("getPhoneFirmwareVersion:  "
				+ _l.getPhoneFirmwareVersion());
		LinkedList<String> behaviorList1 = _l.getBehaviorList();
		List<Behavior> bs = _l.getBehaviors();
		for (int i = 0; i < bs.size(); i++) {
			System.out.println(bs.get(i).getData());
			System.out.println(bs.get(i).getValues());

		}
		System.out.println("关东".substring(0, 2));
		System.out.println("关东".length());
	}
}
