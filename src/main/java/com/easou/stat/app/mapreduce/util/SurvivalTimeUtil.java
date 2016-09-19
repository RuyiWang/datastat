package com.easou.stat.app.mapreduce.util;

import java.util.List;

public class SurvivalTimeUtil {
	public static String	ZERO	= "0";
	public static String	ONE		= "1";

	/**
	 * @Title: getSurvivalTime
	 * @Description: 对于排好序的时间序列，进行计算时长。
	 * @param s
	 * @return
	 * @author 邓长春
	 * @return Long
	 * @throws
	 */
	public static Long getSurvivalTime(List<SurvivalTimeUnit> s) {
		Long st = 0L;
		if (s.size() == 0 || s.size() == 1) {
			return st;
		}
		Long startTime = 0L;
		// 取得第一个开始时间
		for (SurvivalTimeUnit e : s) {
			if (ONE.equals(e.getTag())) {
				startTime = e.getTime();
				break;
			}
		}
		for (int i = 0; i < s.size(); i++) {
			if (ONE.equals(s.get(i).getTag())) {
				startTime = s.get(i).getTime();
			}
			if (ZERO.equals(s.get(i).getTag())) {
				if (i < s.size() - 1) {
					if (ONE.equals(s.get(i + 1).getTag())) {
						st += s.get(i).getTime() - startTime;
					}
				} else {
					if (s.get(i).getTime() > startTime && startTime != 0) {
						st += s.get(i).getTime() - startTime;
					}

				}
			}
		}
		return st;
	}

}
