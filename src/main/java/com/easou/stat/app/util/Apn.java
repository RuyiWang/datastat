package com.easou.stat.app.util;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.easou.stat.app.constant.PhoneApn;

/**
 * @ClassName: PhoneApn
 * @Description: 运营商
 * @author vic
 * @date Oct 9, 2012 5:10:25 PM
 * 
 */
public class Apn {
	static final Map<String, String> phoneApnMaps = new HashMap<String, String>() {
		private static final long serialVersionUID = -2450731159514942289L;
		{
			put("中国联通", PhoneApn.联通.toString());
			put("China Unicom", PhoneApn.联通.toString());
			put("CU-GSM", PhoneApn.联通.toString());
			put("CHN-CUGSM", PhoneApn.联通.toString());

			put("China Telecom", PhoneApn.电信.toString());
			put("ctnet", PhoneApn.电信.toString());
			put("中国电信", PhoneApn.电信.toString());

			put("中国移动-3G", PhoneApn.移动.toString());
			put("中国移动", PhoneApn.移动.toString());
			put("CHINA  MOBILE", PhoneApn.移动.toString());
			put("CMCC", PhoneApn.移动.toString());
			put("中国移动 3G", PhoneApn.移动.toString());
			put("CHINA MOBILE", PhoneApn.移动.toString());
			put("China Mobile", PhoneApn.移动.toString());
			put("中國移動", PhoneApn.移动.toString());
		}
	};

	public static String getPhoneApn(String apn) {
		String phoneApn = PhoneApn.其它.toString();
		if (StringUtils.isNotEmpty(apn)) {
			if (apn.indexOf(",") > 0) {
				// 多个获取第一个
				phoneApn = phoneApnMaps.get(apn.split(",")[0]);
			} else {
				phoneApn = phoneApnMaps.get(apn);
			}
			if (null == phoneApn) {
				phoneApn = PhoneApn.其它.toString();
			}
		}
		return phoneApn;
	}

	public static void main(String[] args) throws ParseException {
		System.out.println(Apn.getPhoneApn("ctnet"));
	}
}
