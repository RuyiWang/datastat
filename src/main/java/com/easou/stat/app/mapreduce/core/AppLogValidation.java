package com.easou.stat.app.mapreduce.core;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nutz.json.Json;
/**
 * @ClassName: AppLogValidation.java
 * @Description: 检查日志是否合法
 * @author: Asa
 * @date: 2014年6月5日 上午11:15:17
 */
public class AppLogValidation {
	public static final Log LOG_MR = LogFactory.getLog(AppLogValidation.class);

	/**
	 * @Titile: isValidateActivityLog
	 * @Description: 是否合法activity日志
	 * @author: Asa
	 * @date: 2014年6月5日 上午11:23:01
	 * @return boolean
	 */
	public static boolean isValidateActivityLog(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}
		Map<String, Object> lineMap = null;
		try {
			lineMap = Json.fromJson(Map.class, str);
		} catch (Exception e) {
			return false;
		}

		String appkey = (String) lineMap.get("appkey");
		String phone_udid = (String) lineMap.get("phone_udid");
		String phone_esid = (String) lineMap.get("phone_esid");
		String cpid = (String) lineMap.get("cpid");
		String phone_softversion = (String) lineMap.get("phone_softversion");

		if (StringUtils.isBlank(appkey) || StringUtils.isBlank(phone_udid)
				|| StringUtils.isBlank(phone_esid) || StringUtils.isBlank(cpid)
				|| StringUtils.isBlank(phone_softversion)) {
			return false;
		}

		return true;
	}

	/**
	 * @Titile: isValidateShareLog
	 * @Description: 是否合法的分享日志
	 * @author: Asa
	 * @date: 2014年6月5日 上午11:20:55
	 * @return boolean
	 */
	public static boolean isValidateShareLog(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}
		Map<String, Object> lineMap = null;
		try {
			lineMap = Json.fromJson(Map.class, str);
		} catch (Exception e) {
			return false;
		}

		String appkey = (String) lineMap.get("appkey");
		String phone_udid = (String) lineMap.get("phone_udid");
		String cpid = (String) lineMap.get("cpid");
		String phone_softversion = (String) lineMap.get("phone_softversion");

		if (StringUtils.isBlank(appkey) || StringUtils.isBlank(phone_udid)
				|| StringUtils.isBlank(cpid)
				|| StringUtils.isBlank(phone_softversion)) {
			return false;
		}

		return true;
	}
}
