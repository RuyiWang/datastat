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

public class PageVisitReducerCollectorDefine implements DBWritable, Writable {

	/** @Fields jobid : Job ID **/
	private String jobid;

	/** @Fields time : 激活的时间 **/
	private String time;

	/** @Fields appkey : 客户端 key **/
	private String appkey;

	/** @Fields dim_type : 维度类型 **/
	private String dim_type;

	/** @Fields desActivity:源activity **/
	private String desActivity;

	/** @Fields targetActivity : 目标activity **/
	private String targetActivity;

	/**@Fields:indicator : 指标 **/
	private int indicator;

	/**@Fields value: 值 **/
	private float value;

	/** @Fields dimensions_code : 统计维度组合 **/
	private String dimensions_code;

	/** @Fields extension_code : 扩展维度值，主要用于三个维度交叉的情况 **/
	private String dimensions_ext2;

	/** @Fields dimensions_ext : 用于四个维度交叉扩展 **/
	private String dimensions_ext1;


	public PageVisitReducerCollectorDefine(String jobid, String time,
			String appkey, String dim_type, String desActivity,
			String targetActivity, int indicator, float value,
			String dimensions_code, String dimensions_ext1,
			String dimensions_ext2) {
		this.jobid = jobid;
		this.time = time;
		this.dim_type = dim_type;
		this.appkey = appkey;
		this.desActivity = desActivity;
		this.targetActivity = targetActivity;
		this.indicator = indicator;
		this.value = value;
		this.dimensions_code = dimensions_code;
		this.dimensions_ext1 = dimensions_ext1;
		this.dimensions_ext2 = dimensions_ext2;
	}

	public static String getTableNameByGranularity(Granularity granularity) {
		switch (granularity) {
		case DAY:
			return "STAT_USER_VISIT_PAGE_DAY";
		case WEEK:
			return "STAT_USER_VISIT_PAGE_WEEK";
		case MONTH:
			return "STAT_USER_VISIT_PAGE_MONTH";
		default:
			return "STAT_USER_VISIT_PAGE";
		}
	}

	public String toString() {
		return "jobid:" + jobid + "  time: " + time + "   appkey:" + appkey
				+ "   dim_type:" + dim_type + "   desActivity:" + desActivity
				+ "   targetActivity:" + targetActivity + "   indicator:"
				+ indicator + "   value:" + value + "   dimensions_code:"
				+ dimensions_code + "   dimensions_ext1:" + dimensions_ext1
				+ "   dimensions_ext2:" + dimensions_ext2;
	}

	public static String[] getFields() {
		return new String[] { "jobid", "time", "appkey", "dim_type",
				"desActivity", "targetActivity", "indicator",
				"value", "dimensions_code","dimensions_ext1","dimensions_ext2"};
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.jobid = Text.readString(in);
		this.time = Text.readString(in);
		this.appkey = Text.readString(in);
		this.dim_type = Text.readString(in);
		this.desActivity = Text.readString(in);
		this.targetActivity = Text.readString(in);
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
		Text.writeString(out, this.desActivity);
		Text.writeString(out, this.targetActivity);
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
		this.desActivity = rs.getString("desActivity");
		this.targetActivity = rs.getString("targetActivity");
		this.indicator = rs.getInt("indicator");
		this.value = rs.getFloat("value");
		this.dimensions_code = rs.getString("dimensions_code");
		this.dimensions_ext1 = rs.getString("dimensions_ext1");
		this.dimensions_ext1 = rs.getString("dimensions_ext2");
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, jobid);
		ps.setString(2, time);
		ps.setString(3, appkey);
		ps.setString(4, dim_type);
		ps.setString(5, this.desActivity);
		ps.setString(6, this.targetActivity);
		ps.setInt(7, indicator);
		ps.setFloat(8, value);
		ps.setString(9, dimensions_code);
		ps.setString(10, dimensions_ext1);
		ps.setString(11, dimensions_ext2);
	}

}
