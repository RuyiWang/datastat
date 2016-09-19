package com.easou.stat.app.mapreduce.core;

import com.easou.stat.app.util.CommonUtils;

public class Activity {

	/** @Fields currentActivity : 当前activity **/
	private String				currentActivity;

	/** @Fields referActivity : 上一个activity **/
	private String				referActivity;

	/** @Fields time : 时间 **/
	private String time;

	public String getCurrentActivity() {
		return currentActivity;
	}

	public void setCurrentActivity(String currentActivity) {
		this.currentActivity = currentActivity;
	}

	public String getReferActivity() {
		return referActivity;
	}

	public void setReferActivity(String referActivity) {
		this.referActivity = referActivity;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}
	
	public String getStrTime() {
		return CommonUtils.getDateTime(time, "yyyyMMddHHmm");
	}

	
	
}
