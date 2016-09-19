package com.easou.stat.app.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SearchDimensions
 * @Description: 纬度类型枚举
 * @author moon
 */
public enum EventDimensions {
	evttimes, //事件次数
	evtudids;	//事件用户数

	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}

	public List<String> getNameList() {
		List<String> s = new ArrayList<String>();
		for (EventDimensions si : EventDimensions.values()) {
			s.add(si.toString());
		}
		return s;
	}

}