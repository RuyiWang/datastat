package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.nutz.json.Json;

import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.ActivityDimensions;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogValidation;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.LongKey;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityRevisitUsersMapReduce1.java
 * @Description: 回访用户分析MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:31:47
 *
 */
public class ActivityRevisitUsersMapReduce1 extends Configured implements Tool{
	public static final Log LOG_MR = LogFactory.getLog(ActivityRevisitUsersMapReduce1.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "false"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map), false);

		Job job = op.getJob();
		String path = "/runtime/tmp/client/"
				+ CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/")
				+ java.util.UUID.randomUUID().toString();
		FileInputFormat.setInputPaths(job, op.getPaths());
		FileOutputFormat.setOutputPath(job, new Path(path));

		job.setMapperClass(ActivityRevisitUsers1Mapper.class);
		job.setReducerClass(ActivityRevisitUsers1Reducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(LongKey.GroupingComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(LongKey.class);
		job.setMapOutputValueClass(Text.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		LOG_MR.info("预处理数据的输出路径为" + path);
		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			op.execute(args[0], path);
		}
		return 0;
	}

	
	public static class ActivityRevisitUsers1Mapper extends Mapper<LongWritable, Text, LongKey, Text> {
		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		private final Calendar calendar= Calendar.getInstance();
		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if(!AppLogValidation.isValidateActivityLog(_line.toString())){
				return;
			};
			JSONObject json = null;
			try{
				json = JSONObject.parseObject(_line.toString());
			}catch(Exception e){
				return;
			}
			if(json == null)
				return;
			String server_time = json.getString("server_time");
			if(StringUtils.isBlank(server_time))
				return;
			String phone_udid = json.getString("phone_udid");
			String appkey = json.getString("appkey");
			if(StringUtils.isBlank(appkey))
				return;
			server_time = server_time.substring(0, 8);
			try {
				calendar.setTime(sdf.parse(server_time));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long time = calendar.getTimeInMillis();
			json.put("server_time", server_time);
			MapReduceHelpler.outputLongKeyValue(appkey+Constant.DELIMITER+phone_udid, time, json.toJSONString(), context);
			
		}
		
	}
	
	public static class ActivityRevisitUsers1Reducer extends Reducer<LongKey, Text, Text, NullWritable>{

		private final Text result = new Text();
		@Override
		protected void reduce(LongKey key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			
			StringBuffer user_revisit =new StringBuffer();
			String word = "";
			String startLine = "";
			boolean flag = true;
			for(Text value : values){
				if(flag == true){
					startLine = value.toString();
					flag = false;
				}
				JSONObject jsb = JSONObject.parseObject(value.toString());
				String up_time  = jsb.getString("server_time");
				if(!word.equals(up_time)) {
					word = up_time;
					user_revisit.append(up_time).append(",");
					JSONObject start = JSONObject.parseObject(startLine);
					start.put(ActivityDimensions.user_revisit.toString(), user_revisit.substring(0, user_revisit.length()-1));
					result.set(start.toJSONString());
					context.write(result, NullWritable.get());
				}
			}
			
			
		}
	}
	
}
