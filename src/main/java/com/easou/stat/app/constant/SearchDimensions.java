package com.easou.stat.app.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SearchDimensions
 * @Description: 纬度类型枚举
 * @author moon
 */
public enum SearchDimensions {
	stimes, //总数
	sact1times,//主动搜索次数
	sact0times,//被动搜索次数
	s1times,//成功次数
	s0times,//失败次数
	swords,//搜索词量
	sswords,//来源词量
	sudids,//搜索用户
	sptimes,//搜索页次
	forbiddens;//禁词搜索数

	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}

	public List<String> getNameList() {
		List<String> s = new ArrayList<String>();
		for (SearchDimensions si : SearchDimensions.values()) {
			s.add(si.toString());
		}
		return s;
	}

}