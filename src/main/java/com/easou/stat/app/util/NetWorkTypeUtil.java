package com.easou.stat.app.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class NetWorkTypeUtil {
	private static Map<String, String> map = new HashMap<String, String>();
	static {
		init();
	}
	public NetWorkTypeUtil() {
	}
	private static void init() {
		map.put("2gnet", "2G");
		map.put("2gwap", "2G");
		map.put("2g", "2G");

		map.put("3g", "3G");
		map.put("3gnet", "3G");
		map.put("3gwap", "3G");

		map.put("wifi", "WIFI");

		map.put("wap", "WAP");

		map.put("no network", "NONETWORK");
		map.put("no", "NONETWORK");
	}
	
	public static String getNetWorkType(String key) {
		if(StringUtils.isEmpty(key))
			return "OTHERS";
		String value = map.get(key.toLowerCase());
		if(value == null)
			return "OTHERS";
		return value;
	}
	
	public static void main(String[] args) {
		System.out.println(getNetWorkType("Wap"));
		System.out.println(getNetWorkType("No network"));
		System.out.println(getNetWorkType("2G"));
		System.out.println(getNetWorkType(""));
	}
}
