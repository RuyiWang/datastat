package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.ActivityDimensions;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorStartDist;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityStartDistMapReduce2.java
 * @Description: 启动次数分步MR step-2
 * @author: Asa
 * @date: 2014年5月13日 下午4:33:08
 *
 */
public class ActivityStartDistMapReduce2 extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityStartDistMapReduce2.class);
	
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
		DBOutputFormat.setOutput(job, ReducerCollectorStartDist.getTableNameByGranularity(op.getGranularity()),ReducerCollectorStartDist.getFields());
		job.setMapperClass(ActivityStartDist2Mapper.class);
		job.setReducerClass(ActivityStartDist2Reducer.class);

		
		job.setOutputKeyClass(ReducerCollectorStartDist.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		return (job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class ActivityStartDist2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		private long time = 0L;
		private String granularity = null;
		
		@Override
		protected void map(LongWritable key,Text _line,Context context)	
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString()))
				return;
			String dateFormat = "yyyyMMddHHmmss";
			Date date = null;
			String peroid = "0";
			if (Granularity.HOUR.toString().equals(granularity)) {
				date = DateUtil.getFirstMinuteOfHour(time);
			} else if (Granularity.DAY.toString().equals(granularity)) {
				date = DateUtil.getFirstHourOfDay(time);
				peroid = "1";
			} else if (Granularity.WEEK.toString().equals(granularity)) {
				date = DateUtil.getMondayOfWeek(time);
				peroid = "7";
			} else {
				date = DateUtil.getFirstDayOfMonth(time);
				peroid = "30";
			}
			String dateTime = CommonUtils.getDateTime(date.getTime(),
					dateFormat);

			JSONObject jsonObj = JSONObject.parseObject(_line.toString());
			String appkey = jsonObj.getString("appkey");
			String phone_softversion = jsonObj.getString("phone_softversion");
			String phone_udid = jsonObj.getString("phone_udid");
			String cpid = jsonObj.getString("cpid");
			String startRange = jsonObj.getString("startRange");
			
			
			context.write(new Text(appkey+Constant.DELIMITER+phone_softversion+
					Constant.DELIMITER+cpid+Constant.DELIMITER+dateTime+
					Constant.DELIMITER+startRange+Constant.DELIMITER+peroid),new Text(phone_udid));
			
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
				e.printStackTrace();
			}
		}
	}

	public static class ActivityStartDist2Reducer extends Reducer<Text,Text,ReducerCollectorStartDist,NullWritable>{
		private String jobId = null;
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			long users = 0;
			for(Text valueText:values){
				users++;
			}
			String[] keyStr = key.toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String phone_softversion = keyStr[1];
			String cpid = keyStr[2];
			String datetime = keyStr[3];
			String startRange = keyStr[4];
			String period = keyStr[5];
			ReducerCollectorStartDist outData = new ReducerCollectorStartDist();
			outData.setJobid(jobId);
			outData.setAppkey(appKey);
			outData.setPhone_softversion(phone_softversion);
			outData.setStat_date(datetime);
			outData.setTime_peroid(period);
			outData.setExtend_dimcode(startRange);
			outData.setDim_type("cpid");
			outData.setDim_code(cpid);
			outData.setValue(users);
			outData.setIndicator(ActivityDimensions.starttimes.toString());
			
			context.write(outData, NullWritable.get());
		}
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
}
