package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
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
import org.apache.hadoop.util.ToolRunner;
import org.nutz.json.Json;

import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogValidation;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.util.DimUtil;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityStartDistMapReduce1.java
 * @Description: 启动次数分步MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:32:30
 *
 */
public class ActivityStartDistMapReduce1 extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityStartDistMapReduce1.class);
	
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
		job.setMapperClass(ActivityStartDist1Mapper.class);
		job.setReducerClass(ActivityStartDist1Reducer.class);
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		LOG_MR.info("预处理数据的输出路径为" + path);
		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			map.put(Constant.APP_TMP_PATH, path);
			map.put("mapReduceClassName", "ActivityStartDistMapReduce2");
			ToolRunner.run(new Configuration(), new ActivityStartDistMapReduce2(), new String[]{Json.toJson(map)});
		}
		return 0;
		}
	
	public static class ActivityStartDist1Mapper extends Mapper<LongWritable, Text, TextKey, Text>{
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
			
			String appkey = json.getString("appkey");
			String phone_softversion = json.getString("phone_softversion");
			String cpid = json.getString("cpid");
			if(cpid.length()> 24){
				cpid = cpid.substring(0, 24);
			}
			String phone_udid = json.getString("phone_udid") ;
			String phone_esid = json.getString("phone_esid");
			List<String> dims = DimUtil.composeDims(null, appkey, false);
			dims = DimUtil.composeDims(dims, phone_softversion, true);
			dims = DimUtil.composeDims(dims, cpid, true);
			dims = DimUtil.composeDims(dims, phone_udid, false);
			for(String dim : dims){
				Text textValue = new Text(phone_esid);
				TextKey textKey = new TextKey(new Text(dim), new Text(phone_esid));
				context.write(textKey, textValue);
			}
			
			
		}
		
		
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
	
		}
	}
	
	public static class ActivityStartDist1Reducer extends
	Reducer<TextKey, Text, Text, NullWritable> {
		
		@Override
		protected void reduce(TextKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			int starttimes = 0;
			String word = "";
			for(Text value : values) {
				if(!word.equals(value.toString())) {
					starttimes ++;
					word = value.toString();
				}
			}
						
			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String phone_softversion = keyStr[1];
			String cpid = keyStr[2];
			String phone_udid = keyStr[3];
			JSONObject JSONKey = new JSONObject();
			JSONKey.put("appkey", appKey);
			JSONKey.put("phone_softversion", phone_softversion);
			JSONKey.put("cpid", cpid);
			JSONKey.put("phone_udid", phone_udid);
			JSONKey.put("starttimes", starttimes);
			if(starttimes<2){
				JSONKey.put("startRange", 1);
			}else if(starttimes<4&&starttimes>1){
				JSONKey.put("startRange", 2);
			}else if(starttimes<6&&starttimes>3){
				JSONKey.put("startRange", 3);
			}else if(starttimes<11&&starttimes>5) {
				JSONKey.put("startRange", 4);
			}else if(starttimes<51&&starttimes>10) {
				JSONKey.put("startRange", 5);
			}else{
				JSONKey.put("startRange", 6);
			}
			context.write(new Text(JSONKey.toJSONString()), NullWritable.get());
			
			
			
			
		}
	}
}
