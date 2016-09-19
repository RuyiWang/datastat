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
 * @ClassName: ReducerCollectorActUsers.java
 * @Description: 活跃用户
 * @author: Asa
 * @date: 2014年5月22日 下午5:34:25
 *
 */
public class ReducerCollectorActUsers implements DBWritable, Writable {
	/** @Fields jobid : Job ID **/
	private String jobid;

	private String stat_date;

	/** @Fields cpid : 推广渠道编码 **/
	private String cpid;

	/** @Fields appkey : 客户端 key **/
	private String appkey;

	/** @Fields n_schn_fullcid : 12位渠道编码**/
	private String n_schn_fullcid;
	
	private String phone_udid;

	private String phone_softversion;


	public ReducerCollectorActUsers() {
		super();
	}

	public static String getTableNameByGranularity(Granularity granularity) {
		switch (granularity) {
		case DAY:
			return "UD_ACT_BASE_INFO_DAY";
		case WEEK:
			return "UD_ACT_BASE_INFO_WEEK";
		default:
			return "UD_ACT_BASE_INFO_MONTH";
		}
	}

	public static String[] getFields() {
		return new String[] { "APPKEY", "PHONE_SOFTVERSION",
				"STAT_DATE", "CPID", "N_SCHN_FULLCID",
				"PHONE_UDID", "JOBID" };
	}
	
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		this.appkey = rs.getString("appkey");
		this.phone_softversion = rs.getString("phone_softversion");
		this.stat_date = rs.getString("stat_date");
		this.cpid = rs.getString("cpid");
		this.n_schn_fullcid = rs.getString("n_schn_fullcid");
		this.phone_udid = rs.getString("phone_udid");
		this.jobid = rs.getString("jobid");
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, this.appkey);
		ps.setString(2, this.phone_softversion);
		ps.setString(3, this.stat_date);
		ps.setString(4, this.cpid);
		ps.setString(5, this.n_schn_fullcid);
		ps.setString(6, this.phone_udid);
		ps.setString(7, this.jobid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.appkey = Text.readString(in);
		this.phone_softversion = Text.readString(in);
		this.stat_date = Text.readString(in);
		this.cpid = Text.readString(in);
		this.n_schn_fullcid = Text.readString(in);
		this.phone_udid = Text.readString(in);
		this.jobid = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.appkey);
		Text.writeString(out, this.phone_softversion);
		Text.writeString(out, this.stat_date);
		Text.writeString(out, this.cpid);
		Text.writeString(out, this.n_schn_fullcid);
		Text.writeString(out, this.phone_udid);
		Text.writeString(out, this.jobid);
	}
	public ReducerCollectorActUsers(String jobid, String stat_date,
			String cpid, String appkey, String n_schn_fullcid, String phone_udid,
			String phone_softversion) {
		super();
		this.jobid = jobid;
		this.stat_date = stat_date;
		this.cpid = cpid;
		this.appkey = appkey;
		this.n_schn_fullcid = n_schn_fullcid;
		this.phone_udid = phone_udid;
		this.phone_softversion = phone_softversion;
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

	public String getCpid() {
		return cpid;
	}

	public void setCpid(String cpid) {
		this.cpid = cpid;
	}

	public String getAppkey() {
		return appkey;
	}

	public void setAppkey(String appkey) {
		this.appkey = appkey;
	}

	public String getN_schn_fullcid() {
		return n_schn_fullcid;
	}

	public void setN_schn_fullcid(String n_schn_fullcid) {
		this.n_schn_fullcid = n_schn_fullcid;
	}

	public String getPhone_udid() {
		return phone_udid;
	}

	public void setPhone_udid(String phone_udid) {
		this.phone_udid = phone_udid;
	}

	public String getPhone_softversion() {
		return phone_softversion;
	}

	public void setPhone_softversion(String phone_softversion) {
		this.phone_softversion = phone_softversion;
	}

	@Override
	public String toString() {
		return "ReducerCollectorSearch [appkey=" + appkey + ", cpid="
				+ cpid + ", n_schn_fullcid=" + n_schn_fullcid + ", phone_udid="
				+ phone_udid + ", jobid=" + jobid + ", phone_softversion="
				+ phone_softversion + ", stat_date=" + stat_date + "]";
	}

}

