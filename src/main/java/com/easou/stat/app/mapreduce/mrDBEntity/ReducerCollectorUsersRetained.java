package com.easou.stat.app.mapreduce.mrDBEntity;

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
 * 
 * @Title: ReducerCollectorUsersRetained.java
 * @Description: 用户留存
 * @author: ASA
 * @date: 2013年8月5日 上午9:54:35
 */
public class ReducerCollectorUsersRetained implements DBWritable, Writable {

	private String jobid;
	private String stat_date;
	private String appkey;
	private String phone_softversion;
	private String dim_type;
	private String dim_code;
	private String indicator;
	private String time_peroid;
	private int value;

	public ReducerCollectorUsersRetained() {
		super();
	}

	public static String getTableNameByGranularity() {
		return "STAT_USER_RETAINED_INFO";
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		this.appkey = rs.getString("appkey");
		this.phone_softversion = rs.getString("phone_softversion");
		this.stat_date = rs.getString("stat_date");
		this.dim_type = rs.getString("dim_type");
		this.dim_code = rs.getString("dim_code");
		this.time_peroid = rs.getString("time_peroid");
		this.indicator = rs.getString("indicator");
		this.jobid = rs.getString("jobid");
		this.value = rs.getInt("value");
	}

	public static String[] getFields() {
		return new String[] { "APPKEY", "PHONE_SOFTVERSION", "STAT_DATE",
				"DIM_TYPE", "DIM_CODE", "TIME_PEROID", "INDICATOR", "JOBID",
				"VALUE" };
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, this.appkey);
		ps.setString(2, this.phone_softversion);
		ps.setString(3, this.stat_date);
		ps.setString(4, this.dim_type);
		ps.setString(5, this.dim_code);
		ps.setString(6, this.time_peroid);
		ps.setString(7, this.indicator);
		ps.setString(8, this.jobid);
		ps.setInt(9, this.value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.appkey = Text.readString(in);
		this.phone_softversion = Text.readString(in);
		this.stat_date = Text.readString(in);
		this.dim_type = Text.readString(in);
		this.dim_code = Text.readString(in);
		this.time_peroid = Text.readString(in);
		this.indicator = Text.readString(in);
		this.jobid = Text.readString(in);
		this.value = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.appkey);
		Text.writeString(out, this.phone_softversion);
		Text.writeString(out, this.stat_date);
		Text.writeString(out, this.dim_type);
		Text.writeString(out, this.dim_code);
		Text.writeString(out, this.time_peroid);
		Text.writeString(out, this.indicator);
		Text.writeString(out, this.jobid);
		out.writeInt(this.value);
	}

	public ReducerCollectorUsersRetained(String jobid, String stat_date,
			String dim_type, String appkey, String dim_code, String indicator,
			String time_peroid, int value, String phone_softversion) {
		super();
		this.jobid = jobid;
		this.stat_date = stat_date;
		this.dim_type = dim_type;
		this.appkey = appkey;
		this.dim_code = dim_code;
		this.indicator = indicator;
		this.phone_softversion = phone_softversion;
		this.time_peroid = time_peroid;
		this.value = value;
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public String getStat_date() {
		return stat_date;
	}

	public void setStat_date(String stat_date) {
		this.stat_date = stat_date;
	}

	public String getAppkey() {
		return appkey;
	}

	public void setAppkey(String appkey) {
		this.appkey = appkey;
	}

	public String getPhone_softversion() {
		return phone_softversion;
	}

	public void setPhone_softversion(String phone_softversion) {
		this.phone_softversion = phone_softversion;
	}

	public String getDim_type() {
		return dim_type;
	}

	public void setDim_type(String dim_type) {
		this.dim_type = dim_type;
	}

	public String getDim_code() {
		return dim_code;
	}

	public void setDim_code(String dim_code) {
		this.dim_code = dim_code;
	}

	public String getIndicator() {
		return indicator;
	}

	public void setIndicator(String indicator) {
		this.indicator = indicator;
	}

	public String getTime_peroid() {
		return time_peroid;
	}

	public void setTime_peroid(String time_peroid) {
		this.time_peroid = time_peroid;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "ReducerCollectorSearch [appkey=" + appkey + ", dim_type="
				+ dim_type + ", dimn_code=" + dim_code + ", indicator="
				+ indicator + ", jobid=" + jobid + ", phone_softversion="
				+ phone_softversion + ", stat_date=" + stat_date
				+ ", time_peroid=" + time_peroid + ", value=" + value + "]";
	}

}
