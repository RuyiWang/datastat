package com.easou.stat.app.mapreduce.po;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.easou.stat.app.constant.Granularity;

/**
 * @ClassName: ReducerCollectorDefine
 * @Description: 自定义客户端事件统计入库类
 * @author 邓长春
 * @date 2012-10-19 下午02:12:17
 * 
 */
public class ReducerCollectorDefine implements DBWritable, Writable {

	private String	jobID;

	private String	eventID;

	private String	condition;

	private String	appKey;

	private String	time;

	private String	indicator;

	private Float	value	= 0f;

	private String	dimType;

	private String	dimensionsCode;

	private String	dimensionsExt1;

	private String	dimensionsExt2;

	private String	isTop;

	public ReducerCollectorDefine() {
		super();
	}

	public ReducerCollectorDefine(String jobID, String eventID, String condition, String appKey, String time, String indicator, Float value, String dimType, String dimensionsCode, String dimensionsExt1,
			String dimensionsExt2, String isTop) {
		this.jobID = jobID;
		this.eventID = eventID;
		this.condition = condition;
		this.appKey = appKey;
		this.time = time;
		this.indicator = indicator;
		this.value = value;
		this.dimType = dimType;
		this.dimensionsCode = dimensionsCode;
		this.dimensionsExt1 = dimensionsExt1;
		this.dimensionsExt2 = dimensionsExt2;
		this.isTop = isTop;
	}

	public static String getTableNameByGranularity(Granularity granularity) {
		switch (granularity) {
		case HOUR:
			return "USER_DEFINED_EVENT_FATDT0_HOUR";
		case DAY:
			return "USER_DEFINED_EVENT_FATDT0_DAY";
		case WEEK:
			return "USER_DEFINED_EVENT_FATDT0_WEEK";
		case MONTH:
			return "USER_DEFINED_EVENT_FATDT0_MON";
		default:
			return "USER_DEFINED_EVENT_FATDT0";
		}
	}

	public static String[] getFields() {
		return new String[] { "JOBID", "EVENT_ID", "CONDITION", "APPKEY", "TIME", "INDICATOR", "VALUE", "DIM_TYPE", "DIMENSIONS_CODE", "DIMENSIONS_EXT1", "DIMENSIONS_EXT2", "IS_TOP" };
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		this.jobID = rs.getString("JOBID");
		this.eventID = rs.getString("EVENT_ID");
		this.condition = rs.getString("CONDITION");
		this.appKey = rs.getString("APPKEY");
		this.time = rs.getString("TIME");
		this.indicator = rs.getString("INDICATOR");
		this.value = rs.getFloat("VALUE");
		this.dimType = rs.getString("DIM_TYPE");
		this.dimensionsCode = rs.getString("DIMENSIONS_CODE");
		this.dimensionsExt1 = rs.getString("DIMENSIONS_EXT1");
		this.dimensionsExt2 = rs.getString("DIMENSIONS_EXT2");
		this.isTop = rs.getString("IS_TOP");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.jobID = Text.readString(in);
		this.eventID = Text.readString(in);
		this.condition = Text.readString(in);
		this.appKey = Text.readString(in);
		this.time = Text.readString(in);
		this.indicator = Text.readString(in);
		this.value = in.readFloat();
		this.dimType = Text.readString(in);
		this.dimensionsCode = Text.readString(in);
		this.dimensionsExt1 = Text.readString(in);
		this.dimensionsExt2 = Text.readString(in);
		this.isTop = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, jobID);
		Text.writeString(out, eventID);
		Text.writeString(out, condition);
		Text.writeString(out, appKey);
		Text.writeString(out, time);
		Text.writeString(out, indicator);
		out.writeFloat(value);
		Text.writeString(out, dimType);
		Text.writeString(out, dimensionsCode);
		Text.writeString(out, dimensionsExt1);
		Text.writeString(out, dimensionsExt2);
		Text.writeString(out, isTop);
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, jobID);
		ps.setString(2, eventID);
		ps.setString(3, condition);
		ps.setString(4, appKey);
		ps.setString(5, time);
		ps.setString(6, indicator);
		ps.setFloat(7, value);
		ps.setString(8, dimType);
		ps.setString(9, dimensionsCode);
		ps.setString(10, dimensionsExt1);
		ps.setString(11, dimensionsExt2);
		ps.setString(12, isTop);
	}

	public String getJobID() {
		return jobID;
	}

	public void setJobID(String jobID) {
		this.jobID = jobID;
	}

	public String getEventID() {
		return eventID;
	}

	public void setEventID(String eventID) {
		this.eventID = eventID;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getAppKey() {
		return appKey;
	}

	public void setAppKey(String appKey) {
		this.appKey = appKey;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getIndicator() {
		return indicator;
	}

	public void setIndicator(String indicator) {
		this.indicator = indicator;
	}

	public Float getValue() {
		return value;
	}

	public void setValue(Float value) {
		this.value = value;
	}

	public String getDimType() {
		return dimType;
	}

	public void setDimType(String dimType) {
		this.dimType = dimType;
	}

	public String getDimensionsCode() {
		return dimensionsCode;
	}

	public void setDimensionsCode(String dimensionsCode) {
		this.dimensionsCode = dimensionsCode;
	}

	public String getDimensionsExt1() {
		return dimensionsExt1;
	}

	public void setDimensionsExt1(String dimensionsExt1) {
		this.dimensionsExt1 = dimensionsExt1;
	}

	public String getDimensionsExt2() {
		return dimensionsExt2;
	}

	public void setDimensionsExt2(String dimensionsExt2) {
		this.dimensionsExt2 = dimensionsExt2;
	}

	public String getIsTop() {
		return isTop;
	}

	public void setIsTop(String isTop) {
		this.isTop = isTop;
	}

}
