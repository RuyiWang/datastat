package com.easou.stat.app.mapreduce.core;

import com.easou.stat.app.util.CommonUtils;

public class ExceptionInfo {

	/** @Fields stack : 堆栈信息 **/
	private String stack;
	
	/** @Fields type : exception包名+类名 **/
	private String type;
	
	/** @Fields date : exception发生的日期 **/
	private String time;
	
	/** @Fields className : 哪个activity类抛出的异常  **/
	private String className;

	public String getStack() {
		return stack;
	}

	public void setStack(String stack) {
		this.stack = stack;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}
	
	public String getStrTime() {
		return CommonUtils.getDateTime(time, "yyyyMMddHHmm");
	}

}
