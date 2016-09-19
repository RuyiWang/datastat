package com.easou.stat.app.schedule.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.nutz.dao.entity.annotation.Column;
import org.nutz.dao.entity.annotation.Table;

/**
 * @ClassName: UserDefinedEvent
 * @Description: 用户自定义事件
 * @author vic
 * @date Oct 17, 2012 10:23:59 AM
 * 
 */
@Table("USER_DEFINED_EVENT")
public class UserDefinedEvent {

	public static UserDefinedEvent getInstance(ResultSet rs) throws SQLException {
		UserDefinedEvent ude = new UserDefinedEvent();
		ude.eventId = rs.getString("EVENT_ID");
		ude.express = rs.getString("EXPRESS");
		ude.createTime = rs.getDate("CREATE_TIME");
		ude.modifyTime = rs.getDate("MODIFY_TIME");
		ude.createPerson = rs.getString("CREATE_PERSON");
		ude.modifyPerson = rs.getString("MODIFY_PERSON");
		ude.transformId = rs.getString("TRANSFORM_ID");
		ude.eventDesc = rs.getString("EVENT_DESC");
		ude.clientType = rs.getString("CLIENT_TYPE");
		return ude;
	}

	@Column("EVENT_ID")
	private String	eventId;

	@Column("EXPRESS")
	private String	express;

	@Column("CREATE_TIME")
	private Date	createTime;

	@Column("MODIFY_TIME")
	private Date	modifyTime;

	@Column("CREATE_PERSON")
	private String	createPerson;

	@Column("MODIFY_PERSON")
	private String	modifyPerson;

	@Column("TRANSFORM_ID")
	private String	transformId;

	@Column("EVENT_DESC")
	private String	eventDesc;

	@Column("CLIENT_TYPE")
	private String	clientType;

	@Override
	public String toString() {
		return "JobHistory [eventId=" + eventId + ", express=" + express + ", createTime=" + createTime + ", modifyTime=" + modifyTime + ", createPerson=" + createPerson + ", modifyPerson=" + modifyPerson
				+ ", transformId=" + transformId + ", eventDesc=" + eventDesc + ", clientType=" + clientType + "]";
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getExpress() {
		return express;
	}

	public void setExpress(String express) {
		this.express = express;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getModifyTime() {
		return modifyTime;
	}

	public void setModifyTime(Date modifyTime) {
		this.modifyTime = modifyTime;
	}

	public String getCreatePerson() {
		return createPerson;
	}

	public void setCreatePerson(String createPerson) {
		this.createPerson = createPerson;
	}

	public String getModifyPerson() {
		return modifyPerson;
	}

	public void setModifyPerson(String modifyPerson) {
		this.modifyPerson = modifyPerson;
	}

	public String getTransformId() {
		return transformId;
	}

	public void setTransformId(String transformId) {
		this.transformId = transformId;
	}

	public String getEventDesc() {
		return eventDesc;
	}

	public void setEventDesc(String eventDesc) {
		this.eventDesc = eventDesc;
	}

	public String getClientType() {
		return clientType;
	}

	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

}
