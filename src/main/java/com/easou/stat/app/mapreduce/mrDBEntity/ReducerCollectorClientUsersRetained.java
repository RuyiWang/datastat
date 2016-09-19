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

public class ReducerCollectorClientUsersRetained implements DBWritable, Writable {

	/** @Fields jobid : Job ID **/
	private String	jobid;

	/** @Fields time : 激活的时间 **/
	private String	time;

	/** @Fields dim_type : 维度类型 **/
	private String	dim_type;

	/** @Fields appkey : 客户端 key **/
	private String	appkey;

	/** @Fields dimensions_code : 统计维度组合 **/
	private String	dimensions_code;

	/** @Fields extension_code : 扩展维度值，主要用于三个维度交叉的情况 **/
	private String	extension_code;

	/** @Fields dimensions_ext : 用于四个维度交叉扩展 **/
	private String	dimensions_ext;

	/** @Fields indicator : 时间间隔 **/
	private int		time_period;

	/** @Fields value : 用户数 **/
	private Float	number_users	= 0f;

	public ReducerCollectorClientUsersRetained() {
		super();
	}

	public ReducerCollectorClientUsersRetained(String jobid, String time, String dim_type, String appkey, String dimensions_code, String extension_code, String dimensions_ext, int time_period, Float number_users) {
		this.jobid = jobid;
		this.time = time;
		this.dim_type = dim_type;
		this.appkey = appkey;
		this.dimensions_code = dimensions_code;
		this.extension_code = extension_code;
		this.dimensions_ext = dimensions_ext;
		this.time_period = time_period;
		this.number_users = number_users;
	}

	public static String getTableNameByGranularity(Granularity granularity) {
		switch (granularity) {
		case DAY:
			return "STAT_USERS_RETAINED_DAY";
		case WEEK:
			return "STAT_USERS_RETAINED_WEEK";
		case MONTH:
			return "STAT_USERS_RETAINED_MONTH";
		default:
			return "STAT_USERS_RETAINED";
		}
	}

	public String toString() {
		return "jobid:" + jobid + "  time：" + time + "   dim_type：" + dim_type + "   appkey：" + appkey + "   dimensions_code:" + dimensions_code + "   extension_code:" + extension_code + "   dimensions_ext:"
				+ dimensions_ext + "   time_period:" + time_period + "   number_users" + number_users;
	}

	public static String[] getFields() {
		return new String[] { "JOBID", "TIME", "DIM_TYPE", "APPKEY", "DIMENSIONS_CODE", "EXTENSION_CODE", "DIMENSIONS_EXT", "TIME_PERIOD", "NUMBER_USERS" };
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		this.jobid = rs.getString("JOBID");
		this.time = rs.getString("TIME");
		this.dim_type = rs.getString("DIM_TYPE");
		this.appkey = rs.getString("APPKEY");
		this.dimensions_code = rs.getString("DIMENSIONS_CODE");
		this.extension_code = rs.getString("EXTENSION_CODE");
		this.dimensions_ext = rs.getString("DIMENSIONS_EXT");
		this.time_period = rs.getInt("TIME_PERIOD");
		this.number_users = rs.getFloat("NUMBER_USERS");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.jobid = Text.readString(in);
		this.time = Text.readString(in);
		this.dim_type = Text.readString(in);
		this.appkey = Text.readString(in);
		this.dimensions_code = Text.readString(in);
		this.extension_code = Text.readString(in);
		this.dimensions_ext = Text.readString(in);
		this.time_period = in.readInt();
		this.number_users = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, jobid);
		Text.writeString(out, time);
		Text.writeString(out, dim_type);
		Text.writeString(out, appkey);
		Text.writeString(out, dimensions_code);
		Text.writeString(out, extension_code);
		Text.writeString(out, dimensions_ext);
		out.writeInt(time_period);
		out.writeFloat(number_users);
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, jobid);
		ps.setString(2, time);
		ps.setString(3, dim_type);
		ps.setString(4, appkey);
		ps.setString(5, dimensions_code);
		ps.setString(6, extension_code);
		ps.setString(7, dimensions_ext);
		ps.setInt(8, time_period);
		ps.setFloat(9, number_users);
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getDim_type() {
		return dim_type;
	}

	public void setDim_type(String dim_type) {
		this.dim_type = dim_type;
	}

	public String getAppkey() {
		return appkey;
	}

	public void setAppkey(String appkey) {
		this.appkey = appkey;
	}

	public String getDimensions_code() {
		return dimensions_code;
	}

	public void setDimensions_code(String dimensions_code) {
		this.dimensions_code = dimensions_code;
	}

	public String getExtension_code() {
		return extension_code;
	}

	public void setExtension_code(String extension_code) {
		this.extension_code = extension_code;
	}

	public String getDimensions_ext() {
		return dimensions_ext;
	}

	public void setDimensions_ext(String dimensions_ext) {
		this.dimensions_ext = dimensions_ext;
	}

	public int getTime_period() {
		return time_period;
	}

	public void setTime_period(int time_period) {
		this.time_period = time_period;
	}

	public Float getNumber_users() {
		return number_users;
	}

	public void setNumber_users(Float number_users) {
		this.number_users = number_users;
	}

}
