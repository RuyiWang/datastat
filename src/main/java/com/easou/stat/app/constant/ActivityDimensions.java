package com.easou.stat.app.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: ActivityDimensions
 * @Description: 纬度类型枚举
 * @author moon
 */
public enum ActivityDimensions {
	acttimes,		//次数
	acttime,		//停留时间
	reftimes,		//来源次数
	actusers,		//活跃用户数
	starttimes,		//启动次数
	usetime,		//单次使用时长
	pt_usetime,		//平均单次使用时长
	up_traffic,		//平均上传流量
	down_traffic,	//平均下载流量
	page_deep,		//平均下载流量
	start_interval,	//启动间隔
	newusers,		//新增用户
	user_revisit,	//回访用户
	shareusers,		//分析用户数
	sharetimes;		//分析次数

	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}

	public List<String> getNameList() {
		List<String> s = new ArrayList<String>();
		for (ActivityDimensions si : ActivityDimensions.values()) {
			s.add(si.toString());
		}
		return s;
	}

}