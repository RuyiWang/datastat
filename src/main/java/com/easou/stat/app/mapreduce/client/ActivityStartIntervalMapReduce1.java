package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
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
import org.nutz.json.Json;

import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogValidation;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.LongKeyAsc;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityStartIntervalMapReduce1.java
 * @Description: 启动间隔分布MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:33:20
 *
 */
public class ActivityStartIntervalMapReduce1 extends Configured implements Tool{
	public static final Log LOG_MR = LogFactory.getLog(ActivityStartIntervalMapReduce1.class);
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

		job.setMapperClass(ActivityStartIntervalMapper1.class);
		job.setReducerClass(ActivityStartIntervalReducer1.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(LongKeyAsc.GroupingComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(LongKeyAsc.class);
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

	
	public static class ActivityStartIntervalMapper1 extends Mapper<LongWritable, Text, LongKeyAsc, Text> {
		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		private final Calendar calendar= Calendar.getInstance();
		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			//检查日志是否合法
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
			server_time = server_time.substring(0, 8);
			try {
				calendar.setTime(sdf.parse(server_time));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long time = calendar.getTimeInMillis();
			json.put("server_time", time);
			MapReduceHelpler.outputLongKeyValueAsc(appkey+Constant.DELIMITER+phone_udid, time, json.toJSONString(), context);
			
		}
		
	}
	
	public static class ActivityStartIntervalReducer1 extends Reducer<LongKeyAsc, Text, Text, NullWritable>{

		private Calendar calendar;
		private Text result = new Text();
		@Override
		protected void reduce(LongKeyAsc key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			List<Long> startInterval = new ArrayList<Long>();
			long word = 0L;
			boolean flag = true;
			String startLine = null;
			for(Text value : values){
				JSONObject jsb = JSONObject.parseObject(value.toString());
				if(flag == true){
					long isValidateTime  = jsb.getLongValue("server_time");
					Calendar c = Calendar.getInstance();
					c.setTimeInMillis(isValidateTime);
					long extend_code = (this.calendar.getTimeInMillis()-isValidateTime)/1000/3600/24;
					if (extend_code != 7){
						break;
					}else{
						startLine= value.toString();
						flag = false;
					}
						
				}
				long up_time = jsb.getLongValue("server_time");
				if(word != up_time) {
					word = up_time;
					startInterval.add(up_time);
				}
			}
			
			if(StringUtils.isNotBlank(startLine)){
				JSONObject json = JSONObject.parseObject(startLine);
				for ( int i=0; i<startInterval.size(); i++){
					for (int j=0; j<startInterval.size() ; j++){
						long netTime = startInterval.get(j)-startInterval.get(i);
						if(netTime > 0){
							json.put("interval", (netTime/1000/3600/24));
							result.set(json.toJSONString());
							context.write(result, NullWritable.get());
						}
					}
				}
			}
			
		}
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			try {
				Configuration config = context.getConfiguration();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				Date date = sdf.parse(config.get(Constant.DATETIME));
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				this.calendar = c;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	
//	public static void main(String[] args){
//		List<Integer> startInterval = new ArrayList<Integer>();
//		startInterval.add(3);
//		startInterval.add(2);
//		startInterval.add(1);
//		startInterval.add(0);
//		
//		for ( int i=0; i<startInterval.size(); i++){
//			for (int j=0; j<startInterval.size() ; j++){
//				long netTime = startInterval.get(j)-startInterval.get(i);
//				if(netTime > 0){
//					System.out.println(netTime);
//				}
//			}
//		}
//	}

}
