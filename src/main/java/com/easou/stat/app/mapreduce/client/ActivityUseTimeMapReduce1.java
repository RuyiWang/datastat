package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.nutz.json.Json;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogValidation;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityUseTimeMapReduce1.java
 * @Description: 用户使用时长分析MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:35:24
 *
 */
public class ActivityUseTimeMapReduce1 extends Configured implements Tool{
	public static final Log LOG_MR = LogFactory.getLog(ActivityUseTimeMapReduce1.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "false"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map),false);

		Job job = op.getJob();
		String path = "/runtime/tmp/client/"
				+ CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/")
				+ java.util.UUID.randomUUID().toString();
		FileInputFormat.setInputPaths(job, op.getPaths());
		FileOutputFormat.setOutputPath(job, new Path(path));

		job.setMapperClass(ActivityUseTime1Mapper.class);
		job.setReducerClass(ActivityUseTime1Reducer.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		LOG_MR.info("预处理数据的输出路径为" + path);
		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			map.put(Constant.APP_TMP_PATH, path);
			map.put("mapReduceClassName", "ActivityUseTimeMapReduce2");
			ToolRunner.run(new Configuration(), new ActivityUseTimeMapReduce2(), new String[]{Json.toJson(map)});
		}
		return 0;

	}
	
	
	public static class ActivityUseTime1Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		private final static LongWritable result = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			//检查日志是否合法
			if(!AppLogValidation.isValidateActivityLog(_line.toString())){
				return;
			}
			JSONObject jsonObj = null;
			try{
				jsonObj = JSONObject.parseObject(_line.toString());
			}catch(Exception e){
				return;
			}
			if(jsonObj == null)
				return;
			
			String appkey = jsonObj.getString("appkey");
			String phone_softversion = jsonObj.getString("phone_softversion");
			String phone_esid = jsonObj.getString("phone_esid");
			String cpid = jsonObj.getString("cpid");
			if(cpid.length()> 24){
				cpid = cpid.substring(0, 24);
			}
			JSONArray activities = jsonObj.getJSONArray("activities");
			
			if (activities.size() <= 0) {
				return;
			}
			long useTime = 0L;
			for (int i = 0; i < activities.size(); i++) {
				JSONObject activity = activities.getJSONObject(i);
//				if(StringUtils.isBlank(activity.getString("activity_use_time"))){
//					continue;
//				}else{
//					useTime = useTime + activity.getLongValue("activity_use_time");
//				}
				
				if(StringUtils.isNotBlank(activity.getString("activity_use_time"))){
					useTime = useTime + activity.getLongValue("activity_use_time");
				}
				
			}
			result.set(useTime);
			context.write(new Text(appkey+Constant.DELIMITER+phone_softversion+Constant.DELIMITER+
					cpid+Constant.DELIMITER+phone_esid), result);
		}
		
	}
	
	
	
	public static class ActivityUseTime1Reducer extends Reducer<Text, LongWritable, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			for(LongWritable value : values){
				sum += value.get();
			}
			String[] keyStr = key.toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String phone_softversion = keyStr[1];
			String cpid = keyStr[2];
			String phone_esid = keyStr[3];
			JSONObject JSONKey = new JSONObject();
			JSONKey.put("appkey", appKey);
			JSONKey.put("phone_softversion", phone_softversion);
			JSONKey.put("cpid", cpid);
			JSONKey.put("phone_esid", phone_esid);
			JSONKey.put("usetime", sum/1000);
			context.write(new Text(JSONKey.toJSONString()), NullWritable.get());
		}
	}
	

}
