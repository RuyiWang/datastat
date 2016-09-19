package com.easou.stat.app.schedule.db;

public class SaleChannelModel {
	private String id;
	private String firstChannel;
	private String secondChannel;
	private String rule;
	private String fullChannel;
	public SaleChannelModel() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public SaleChannelModel(String id, String firstChannel,
			String secondChannel, String rule, String fullChannel) {
		super();
		this.id = id;
		this.firstChannel = firstChannel;
		this.secondChannel = secondChannel;
		this.rule = rule;
		this.fullChannel = fullChannel;
	}

	public String getFirstChannel() {
		return firstChannel;
	}
	public void setFirstChannel(String firstChannel) {
		this.firstChannel = firstChannel;
	}
	public String getSecondChannel() {
		return secondChannel;
	}
	public void setSecondChannel(String secondChannel) {
		this.secondChannel = secondChannel;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getRule() {
		return rule;
	}
	public void setRule(String rule) {
		this.rule = rule;
	}
	public String getFullChannel() {
		return fullChannel;
	}
	public void setFullChannel(String fullChannel) {
		this.fullChannel = fullChannel;
	}
	@Override
	public String toString() {
		return "SaleChannelModel [firstChannel=" + firstChannel
				+ ", fullChannel=" + fullChannel + ", id=" + id + ", rule="
				+ rule + ", secondChannel=" + secondChannel + "]";
	}
}
