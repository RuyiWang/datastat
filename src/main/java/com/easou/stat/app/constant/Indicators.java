package com.easou.stat.app.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SummaryIndicators
 * @Description: 汇总指标枚举
 * @author 张羽强
 * @date 2012-9-3 下午3:58:26
 * 
 */

public enum Indicators {
	NUMBER_USERS, // 用户数
	NUMBER_NEW_USERS, // 新用户数
	NUMBER_ACITVE_USERS, // 活跃用户数
	NUMBER_RETAINED_USERS, // 用户留存
	TIMES_START, // 启动次数
	TIME_AVERAGE, // 平均时长
	NUMBER_ASC_USERS, // 升级用户数
	NUMBER_NET_TYPE; // 上网方式计数

	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}

	public List<String> getNameList() {
		List<String> s = new ArrayList<String>();
		for (Indicators si : Indicators.values()) {
			s.add(si.toString());
		}
		return s;
	}

}