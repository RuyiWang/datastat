package com.easou.stat.app.schedule.db;

public class EventRuleDbModel {
	private String id;
	private String type;
	private String rules;
	public EventRuleDbModel() {
		super();
		// TODO Auto-generated constructor stub
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getRules() {
		return rules;
	}
	public void setRules(String rules) {
		this.rules = rules;
	}
	@Override
	public String toString() {
		return "EventRuleModel [id=" + id + ", rules=" + rules + ", type="
				+ type + "]";
	}
}
