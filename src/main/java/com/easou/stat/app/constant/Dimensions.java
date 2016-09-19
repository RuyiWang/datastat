package com.easou.stat.app.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: Dimensions
 * @Description: 纬度类型枚举
 * @author vic
 * @date Sep 10, 2012 10:27:50 AM
 * 
 */
public enum Dimensions {
	city, // 地区
	cpid, // 渠道
	version, // 版本
	phone_model, // 机型
	phone_apn, // 运营商
	phone_resolution, // 分辨率
	os, // 操作系统
	osVersion, // 操作系统版本
	currentnetworktype, // 上网方式
	all, // 表示所有
	osAndCpid, // 操作系统+渠道
	versionAndCpid, // 版本+渠道
	osAndosVersion, // 操作系统+操作系统版本
	cpid_code; // 渠道编码

	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}

	public List<String> getNameList() {
		List<String> s = new ArrayList<String>();
		for (Dimensions si : Dimensions.values()) {
			s.add(si.toString());
		}
		return s;
	}

}