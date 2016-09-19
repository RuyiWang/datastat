package com.easou.stat.app.mapreduce.core;

import java.util.Map;

import com.easou.stat.app.util.CommonUtils;

/**
 * @ClassName: Behavior
 * @Description: 用户行为
 * @author vic
 * @date Sep 24, 2012 1:41:18 PM
 * 
 */
public class Behavior {

	/** @Fields op : 行为操作 **/
	private String				op;

	/** @Fields time : 时间 **/
	private String				time;

	/** @Fields data : 类型 （1：属性;0:非属性） **/
	private int					data;

	/** @Fields values : info信息 data为0时获取 **/
	private String				values;

	/** @Fields propertiesValue : 属性信息 data为1时获取 **/
	private Map<String, String>	propertiesValue;

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public int getData() {
		return data;
	}

	public void setData(int data) {
		this.data = data;
	}

	public String getTime() {
		return time;
	}

	public String getStrTime() {
		return CommonUtils.getDateTime(time, "yyyyMMddHHmm");
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getValues() {
		return values;
	}

	public void setValues(String values) {
		this.values = values;
	}

	public Map<String, String> getPropertiesValue() {
		return propertiesValue;
	}

	public void setPropertiesValue(Map<String, String> propertiesValue) {
		this.propertiesValue = propertiesValue;
	}

	/**
	 * @Title: isAppStart
	 * @Description: 获取客户端应用启动标志（1：启动;0:非启动） 1-操作描述 ;0-操作描述
	 * @return
	 * @author vic
	 * @return boolean
	 * @throws
	 */
	public boolean isAppStart() {
		Boolean flag = Boolean.FALSE;
		if (null != op && op.indexOf("-") != -1) {
			if ("1".equals(op.substring(0, 1))) {
				flag = Boolean.TRUE;
			}
		}
		return flag;
	}

	/**
	 * @Title: parseOp
	 * @Description: 解析op返回一个对象。
	 * @return
	 * @author 邓长春
	 * @return OpUnit
	 * @throws
	 */
	public OpUnit parseOp() {
		if (null != op && op.indexOf("-") != -1) {
			String[] opArr = op.split("-");
			if (opArr.length > 2) {
				if ("begin".equals(opArr[2])) {
					return new OpUnit(opArr[0], opArr[1], "1");
				} else if ("end".equals(opArr[2])) {
					return new OpUnit(opArr[0], opArr[1], "0");
				} else {
					return new OpUnit(opArr[0], opArr[1], "");
				}
			} else {
				return new OpUnit(opArr[0], opArr[1], "");
			}
		} else {
			return null;
		}
	}

}
