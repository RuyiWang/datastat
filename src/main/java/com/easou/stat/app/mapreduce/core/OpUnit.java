package com.easou.stat.app.mapreduce.core;

/**
 * @ClassName: OpUnit
 * @Description: 定义一个OpUnit类，方便解析解析op域。
 * @author 邓长春
 * @date 2012-10-17 上午11:00:31
 * 
 */
public class OpUnit {
	private String	startTag;
	private String	eventID;
	private String	timeTag;

	public OpUnit(String startTag, String eventID, String timeTag) {
		this.startTag = startTag;
		this.eventID = eventID;
		this.timeTag = timeTag;
	}

	public String getStartTag() {
		return startTag;
	}

	public void setStartTag(String startTag) {
		this.startTag = startTag;
	}

	public String getEventID() {
		return eventID;
	}

	public void setEventID(String eventID) {
		this.eventID = eventID;
	}

	public String getTimeTag() {
		return timeTag;
	}

	public void setTimeTag(String timeTag) {
		this.timeTag = timeTag;
	}

}
