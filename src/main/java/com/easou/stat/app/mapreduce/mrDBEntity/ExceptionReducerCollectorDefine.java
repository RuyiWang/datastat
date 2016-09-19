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

public class ExceptionReducerCollectorDefine implements DBWritable, Writable {

	/** @Fields jobid : Job ID **/
	private String jobid;

	/** @Fields time : 激活的时间 **/
	private String time;

	/** @Fields appkey : 客户端 key **/
	private String appkey;

	/** @Fields dim_type : 维度类型 **/
	private String dim_type;

	/** @Fields exceptionSign : 异常标识 **/
	private String exceptionSign;

	/** @Fields exceptionDesc : 异常描述 **/
	private String exceptionDesc;

	/** @Fields occureenceTime : 发生时间 **/
	private String occureenceTime;

	/** @Fields isDispose : 0表示未处理，1表示处理过了 **/
	private int isDispose;

	/** @Fields indicator: 指标 **/
	private int indicator;

	/** @Fields value :值 **/
	private float value;

	/** @Fields dimensions_code : 统计维度组合 **/
	private String dimensions_code;

	/** @Fields dimensions_ext : 用于四个维度交叉扩展 **/
	private String dimensions_ext1;

	/** @Fields dimensions_ext : 用于四个维度交叉扩展 **/
	private String dimensions_ext2;

	public ExceptionReducerCollectorDefine(String jobid, String time,
			String appkey, String exceptionSign, String dim_type,
			String exceptionDesc, String occureenceTime, int indicator,
			float value, int isDispose, String dimensions_code,
			String dimensions_ext1, String dimensions_ext2) {
		this.jobid = jobid;
		this.time = time;
		this.appkey = appkey;
		this.exceptionSign = exceptionSign;
		this.dim_type = dim_type;
		this.exceptionDesc = exceptionDesc;
		this.occureenceTime = occureenceTime;
		this.indicator = indicator;
		this.value = value;
		this.isDispose = isDispose;
		this.dimensions_code = dimensions_code;
		this.dimensions_ext1 = dimensions_ext1;
		this.dimensions_ext2 = dimensions_ext2;
	}

	public static String getTableNameByGranularity(Granularity granularity) {
		switch (granularity) {
		case DAY:
			return "STAT_EXCEPTION_DAY";
		case WEEK:
			return "STAT_EXCEPTION_WEEK";
		case MONTH:
			return "STAT_EXCEPTION_MONTH";
		default:
			return "STAT_EXCEPTION";
		}
	}

	public String toString() {
		return "jobid:" + jobid + "  time: " + time + "   appkey:" + appkey
				+ "   dim_type:" + dim_type + "   exceptionSign:"
				+ exceptionSign + "   exceptionDesc:" + exceptionDesc
				+ "   occureenceTime:" + occureenceTime + "   isDispose:"
				+ isDispose + "   indicator:" + indicator + "   value:" + value
				+ "   dimensions_code:" + dimensions_code
				+ "   dimensions_ext1:" + dimensions_ext1
				+ "   dimensions_ext2:" + dimensions_ext2;
	}

	public static String[] getFields() {
		return new String[] { "jobid", "time", "appkey", "dim_type",
				"exceptionSign", "exceptionDesc", "occureenceTime",
				"isDispose", "indicator", "value", "dimensions_code",
				"dimensions_ext1", "dimensions_ext2" };
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.jobid = Text.readString(in);
		this.time = Text.readString(in);
		this.appkey = Text.readString(in);
		this.dim_type = Text.readString(in);
		this.exceptionSign = Text.readString(in);
		this.exceptionDesc = Text.readString(in);
		this.occureenceTime = Text.readString(in);
		this.isDispose = in.readInt();
		this.indicator = in.readInt();
		this.value = in.readFloat();
		this.dimensions_code = Text.readString(in);
		this.dimensions_ext1 = Text.readString(in);
		this.dimensions_ext2 = Text.readString(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.jobid);
		Text.writeString(out, this.time);
		Text.writeString(out, this.appkey);
		Text.writeString(out, this.dim_type);
		Text.writeString(out, this.exceptionSign);
		Text.writeString(out, this.exceptionDesc);
		Text.writeString(out, this.occureenceTime);
		out.writeInt(this.isDispose);
		out.writeInt(this.indicator);
		out.writeFloat(this.value);
		Text.writeString(out, this.dimensions_code);
		Text.writeString(out, this.dimensions_ext1);
		Text.writeString(out, this.dimensions_ext2);

	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		this.jobid = rs.getString("jobid");
		this.time = rs.getString("time");
		this.appkey = rs.getString("appkey");
		this.dim_type = rs.getString("dim_type");
		this.exceptionSign = rs.getString("exceptionSign");
		this.exceptionDesc = rs.getString("exceptionDesc");
		this.occureenceTime = rs.getString("occureenceTime");
		this.isDispose = rs.getInt("isDispose");
		this.indicator = rs.getInt("indicator");
		this.value = rs.getFloat("value");
		this.dimensions_code = rs.getString("dimensions_code");
		this.dimensions_ext1 = rs.getString("dimensions_ext1");
		this.dimensions_ext1 = rs.getString("dimensions_ext2");

	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, this.jobid);
		ps.setString(2, this.time);
		ps.setString(3, this.appkey);
		ps.setString(4, this.dim_type);
		ps.setString(5, this.exceptionSign);
		if (this.exceptionDesc.length() > 4000) {
			ps.setString(6, this.exceptionDesc.substring(0,3999));
		} else {
			ps.setString(6, this.exceptionDesc);
		}
		ps.setString(7, this.occureenceTime);
		ps.setInt(8, this.isDispose);
		ps.setInt(9, indicator);
		ps.setFloat(10, value);
		ps.setString(11, dimensions_code);
		ps.setString(12, dimensions_ext1);
		ps.setString(13, dimensions_ext2);

	}

}
