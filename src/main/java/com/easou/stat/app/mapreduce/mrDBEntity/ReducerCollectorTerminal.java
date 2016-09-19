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

public class ReducerCollectorTerminal implements DBWritable, Writable {
	
	/** @Fields jobid : Job ID **/
	private String jobid;

	private String stat_date;

	/** @Fields appkey : 客户端 key **/
	private String appkey;
	
	private String phone_softversion;
	
	private String cpid;
	
	private String time_peroid;
	
	/** @Fields dim_type : 维度类型 **/
	private String dim_type;

	/** @Fields dimensions_code : 统计维度组合 **/
	private String dim_code;
	
	private String indicator;

	/** @Fields value : 指标值 **/
	private Float value = 0f;

	public ReducerCollectorTerminal() {
		super();
	}
	
	public static String[] getFields() {
		return new String[] { "APPKEY", "PHONE_SOFTVERSION",
				"TIME_PEROID", "STAT_DATE", "DIM_TYPE", "DIM_CODE","CPID",
				"INDICATOR", "VALUE", "JOBID" };
	}
	
	public static String getTableNameByGranularity() {
		
		return "STAT_USER_TERMINAL_INFO_DAY";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.appkey = Text.readString(in);
		this.phone_softversion = Text.readString(in);
		this.time_peroid = Text.readString(in);
		this.stat_date = Text.readString(in);
		this.dim_type = Text.readString(in);
		this.dim_code = Text.readString(in);
		this.cpid = Text.readString(in);
		this.indicator = Text.readString(in);
		this.value = in.readFloat();
		this.jobid = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.appkey);
		Text.writeString(out, this.phone_softversion);
		Text.writeString(out, this.time_peroid);
		Text.writeString(out, this.stat_date);
		Text.writeString(out, this.dim_type);
		Text.writeString(out, this.dim_code);
		Text.writeString(out, this.cpid);
		Text.writeString(out, this.indicator);
		out.writeFloat(this.value);
		Text.writeString(out, this.jobid);
		
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		this.appkey = rs.getString("appkey");
		this.phone_softversion = rs.getString("phone_softversion");
		this.time_peroid = rs.getString("time_peroid");
		this.stat_date = rs.getString("stat_date");
		this.dim_type = rs.getString("dim_type");
		this.dim_code = rs.getString("dim_code");
		this.cpid = rs.getString("cpid");
		this.indicator = rs.getString("indicator");
		this.value = rs.getFloat("value");
		this.jobid = rs.getString("jobid");
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, this.appkey);
		ps.setString(2, this.phone_softversion);
		ps.setString(3, this.time_peroid);
		ps.setString(4, this.stat_date);
		ps.setString(5, this.dim_type);
		ps.setString(6, this.dim_code);
		ps.setString(7, this.cpid);
		ps.setString(8, this.indicator);
		ps.setFloat(9, this.value);
		ps.setString(10, this.jobid);
	}

	
	public ReducerCollectorTerminal(String jobid, String statDate,
			String dimType, String appkey, String dimCode,String cpid, String indicator,
			String phoneSoftversion, String timePeroid, Float value) {
		super();
		this.jobid = jobid;
		this.stat_date = statDate;
		this.dim_type = dimType;
		this.cpid = cpid;
		this.appkey = appkey;
		this.dim_code = dimCode;
		this.indicator = indicator;
		this.phone_softversion = phoneSoftversion;
		this.time_peroid = timePeroid;
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "ReducerCollectorSearch [appkey=" + appkey + ", dim_code="
				+ dim_code+ ", cpid=" + cpid + ", dim_type=" + dim_type + ", indicator="
				+ indicator + ", jobid=" + jobid + ", phone_softversion="
				+ phone_softversion + ", stat_date=" + stat_date
				+ ", time_peroid=" + time_peroid + ", value=" + value + "]";
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

	public String getCpid() {
		return cpid;
	}

	public void setCpid(String cpid) {
		this.cpid = cpid;
	}

	public String getTime_peroid() {
		return time_peroid;
	}

	public void setTime_peroid(String time_peroid) {
		this.time_peroid = time_peroid;
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

	public Float getValue() {
		return value;
	}

	public void setValue(Float value) {
		this.value = value;
	}
	
	
}
