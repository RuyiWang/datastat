package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.nutz.json.Json;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorActUsers;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityUsersRetainedMapRecude.java
 * @Description: 数据库存储计算新用户留存所有的MR
 * @author: Asa
 * @date: 2014年6月5日 上午11:11:32
 *
 */
public class ActivityUsersRetainedMapRecude extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityUsersRetainedMapRecude.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "false"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "false"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map), false);

		Job job = op.getJob();
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorActUsers.getTableNameByGranularity(op.getGranularity()),
				ReducerCollectorActUsers.getFields());

		job.setMapperClass(ActivityUsersRetainedMapper.class);
		job.setReducerClass(ActivityUsersRetainedReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setOutputKeyClass(ReducerCollectorActUsers.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ActivityUsersRetainedMapper extends
			Mapper<LongWritable, Text, TextKey, Text> {

		private long time = 0L;
		private String granularity = null;

		@SuppressWarnings("unchecked")
		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			
			String dateFormat = "yyyyMMddHHmmss";
			Date date = null;
			Map<String, Object> lineMap = Json.fromJson(Map.class, _line.toString());
			//String peroid = "0";
			if (Granularity.HOUR.toString().equals(this.granularity)) {
				date = DateUtil.getFirstMinuteOfHour(this.time);
			} else if (Granularity.DAY.toString().equals(this.granularity)) {
				date = DateUtil.getFirstHourOfDay(this.time);
			//	peroid = "1";
			} else if (Granularity.WEEK.toString().equals(this.granularity)) {
				date = DateUtil.getMondayOfWeek(this.time);
			//	peroid = "7";
			} else {
				date = DateUtil.getFirstDayOfMonth(this.time);
			//	peroid = "30";
			}
			String dateTime = CommonUtils.getDateTime(date.getTime(),
					dateFormat);
			
			String phone_udid = (String) lineMap.get("phone_udid");
			String version = (String) lineMap.get("phone_softversion");
			if( phone_udid == null || "".equals(phone_udid)){
				return;
			}
			
			if(version == null|| "".equals(version))
				return;
			
			String appkey = (String) lineMap.get("appkey");
			if(appkey == null ||"".equals(appkey)){
				return;
			}
			
			String cpid = (String) lineMap.get("cpid");
			if(cpid == null ||"".equals(cpid)){
				return;
			}
			if(cpid.length()> 24){
				cpid = cpid.substring(0, 24);
			}
			write(context,dateTime + Constant.DELIMITER + phone_udid, _line);
		}

		private void write(Context context, String key, Text value)
				throws IOException, InterruptedException {
			TextKey textKey = new TextKey(new Text(key), new Text(""));
			context.write(textKey, value);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration config = context.getConfiguration();
			this.granularity = config.get(Constant.GRANULARITY);
			String dateTime = config.get(Constant._DATETIME);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			try {
				this.time = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				LOG_MR.error("格式化时间错误", e);
			}
		}

	}

	public static class ActivityUsersRetainedReducer extends
			Reducer<TextKey, Text, ReducerCollectorActUsers, NullWritable> {

		private String jobId = null;

		@SuppressWarnings("unchecked")
		protected void reduce(TextKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String[] keyStr = key.getDimension().toString()
					.split(Constant.DELIMITER_REG);
			
			String dateTime = keyStr[0];
			String phone_udid = keyStr[1];
			
			String _line = "";
			long maxTime = 0L;
			
			for(Text value : values ){
				_line = value.toString();
				Map<String, Object> lineMap =  (Map<String, Object>)Json.fromJson(Map.class, value.toString());
				if ("type_behaviorinfo".equalsIgnoreCase((String)lineMap.get("type"))) {
					List<Map<String, Object>> behaviors = (List<Map<String, Object>>) lineMap.get("behavior");
					long tmpMaxTime = 0L;
					if(!behaviors.isEmpty()){
						Map<String, Object> tmp = behaviors.get(0);
						tmpMaxTime = Long.parseLong((String)tmp.get("time"));
					}
					if(tmpMaxTime > maxTime){
						maxTime = tmpMaxTime;
						_line = value.toString();
					}
					
				} else {
					
					List<Map<String, Object>> activities = (List<Map<String, Object>>) lineMap.get("activities");
					long tmpMaxTime = 0L;
					if(!activities.isEmpty()){
						Map<String, Object> tmp = activities.get(0);
						tmpMaxTime = Long.parseLong((String)tmp.get("time"));
					}
					if(tmpMaxTime > maxTime){
						maxTime = tmpMaxTime;
						_line = value.toString();
					}
				}

			}
			
			Map<String, Object> maxMap =  (Map<String, Object>)Json.fromJson(Map.class, _line);
			ReducerCollectorActUsers outData = new ReducerCollectorActUsers();
			outData.setAppkey((String)maxMap.get("appkey"));
			String cpid = (String)maxMap.get("cpid");
			outData.setCpid(cpid);
//			String n_schn_fullcid = SaleChannelUtil.getInstance()
//					.getSaleChannelByCpid(cpid, (String) maxMap.get("os"));
//			outData.setN_schn_fullcid(n_schn_fullcid);
			outData.setJobid(this.jobId);
			outData.setPhone_softversion((String)maxMap.get("phone_softversion"));
			outData.setStat_date(dateTime);
			outData.setPhone_udid(phone_udid);
			context.write(outData, NullWritable.get());
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}

	}

}
