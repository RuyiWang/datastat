package com.easou.stat.app.mapreduce.util;

/**
 * @ClassName: SurvivalTimeUnit
 * @Description: 简单保存操作类型与时间的POJO。
 * @author 邓长春
 * @date 2012-9-28 下午04:05:57
 * 
 */
public class SurvivalTimeUnit {
	/** @Fields time : 时间 **/
	public Long		time;
	/** @Fields tag : 此刻时间应的操作1-启动，0-其他 **/
	public String	tag;

	public SurvivalTimeUnit(Long time, String tag) {
		this.time = time;
		this.tag = tag;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String toString() {
		return "tag:+" + tag + "==time:" + time;
	}

}
