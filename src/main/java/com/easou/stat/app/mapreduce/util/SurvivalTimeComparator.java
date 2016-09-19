package com.easou.stat.app.mapreduce.util;

import java.util.Comparator;

/**
 * @ClassName: SurvivalTimeComparator
 * @Description: 用于对SurvivalTimeUnit进行比较的比较器。
 * @author 邓长春
 * @date 2012-9-28 下午04:06:31
 * 
 */
public class SurvivalTimeComparator implements Comparator {

	@Override
	public int compare(Object o1, Object o2) {
		SurvivalTimeUnit s1 = (SurvivalTimeUnit) o1;
		SurvivalTimeUnit s2 = (SurvivalTimeUnit) o2;
		return s1.getTime() >= s2.getTime() ? 1 : 0;
	}
}