package com.easou.stat.app.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class ApnUtil {
	private static Map<String, String> map = new HashMap<String, String>();
	static {
		init();
	}
	public ApnUtil() {
		
	}
	
	private static void init() {
		map.put("中國移動", "CHINAMOBILE");
		map.put("中国移动-3G", "CHINAMOBILE");
		map.put("中国移动3G", "CHINAMOBILE");
		map.put("中国移动", "CHINAMOBILE");
		map.put("CTC", "CHINAMOBILE");
		map.put("CHN-CUGSM", "CHINAMOBILE");
		map.put("China Mobile", "CHINAMOBILE");
		map.put("CHINA MOBILE 3G", "CHINAMOBILE");
		map.put("CHINA MOBILE", "CHINAMOBILE");
		map.put("CMCC", "CHINAMOBILE");

		map.put("中國聯通", "CHINAUNICOM");
		map.put("中国联通", "CHINAUNICOM");
		map.put("CU-GSM", "CHINAUNICOM");
		map.put("CHN-UNICOM", "CHINAUNICOM");
		map.put("CHN Unicom", "CHINAUNICOM");
		map.put("China Unicom", "CHINAUNICOM");

		map.put("中国电信", "CHINATELECOM");
		map.put("CHINATELECOM", "CHINATELECOM");
		map.put("China Telecom", "CHINATELECOM");
		map.put("ctnet", "CHINATELECOM");
		map.put("46003", "CHINATELECOM");
	}
	
	public static String getNetWorkType(String key) {
		if(StringUtils.isEmpty(key) || "null".equals(key))
			return "UNKNOWN";
		String[] keyStr = key.split(",");
		String tmp = null;
		for(String keySplit : keyStr) {
			if(!"null".equals(keySplit) && StringUtils.isNotEmpty(keySplit)) {
				tmp = keySplit;
				break;
			}
		}
		String value = map.get(tmp);
		if(value == null)
			return "OTHERS";
		return value;
	}
	
	public static void main(String[] args) {
		System.out.println(getNetWorkType("null,46003"));
		System.out.println(getNetWorkType("中國聯通"));
		System.out.println(getNetWorkType("CHINA MOBILE 3G"));
		System.out.println(getNetWorkType(""));
		System.out.println(getNetWorkType("中华电信"));
	}
}
