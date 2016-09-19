package com.easou.stat.app.mapreduce.core;

public class TreeMapKey implements java.io.Serializable{
	
	private int value;
	private String dimission;
	public TreeMapKey(){}
	public TreeMapKey(int value, String dimission) {
		super();
		this.value = value;
		this.dimission = dimission;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public String getDimission() {
		return dimission;
	}
	public void setDimission(String dimission) {
		this.dimission = dimission;
	}

}
