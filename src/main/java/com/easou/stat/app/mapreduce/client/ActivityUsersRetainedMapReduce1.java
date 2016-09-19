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
import com.easou.stat.app.constant.AppKey;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.AppLogValidation;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.IntKey;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityUsersRetainedMapReduce1.java
 * @Description: 新用户留存计算-step1
 * @author: Asa
 * @date: 2014年6月5日 上午11:02:07
 *
 */
public class ActivityUsersRetainedMapReduce1 extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityUsersRetainedMapReduce1.class);

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

		job.setMapperClass(ActivityUsersRetained1Mapper.class);
		job.setReducerClass(ActivityUsersRetained1Reducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(IntKey.GroupingComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(IntKey.class);
		job.setMapOutputValueClass(Text.class);
		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		LOG_MR.info("预处理数据的输出路径为" + path);
		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			map.put(Constant.APP_TMP_PATH, path);
			map.put("mapReduceClassName", "ActivityUsersRetainedMapReduce2");
			ToolRunner.run(new Configuration(), new ActivityUsersRetainedMapReduce2(), new String[]{Json.toJson(map)});
		}
		return 0;
	}

	public static class ActivityUsersRetained1Mapper extends
			Mapper<LongWritable, Text, IntKey, Text> {
		@Override
		protected void map(LongWritable l, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			AppLogFormat _l = new AppLogFormat(_line.toString());
			if (_l.isMobilePhoneInfo()) {
				MapReduceHelpler.outputIntKeyValue(
						_l.getOpenudid() + _l.getAppkey(),
						Constant.INT_COUNT_ONE, _line.toString(), context);
			} else if (_l.isActivityInfo()) {
				if (AppLogValidation.isValidateActivityLog(_line.toString())) {
					MapReduceHelpler.outputIntKeyValue(
							_l.getOpenudid() + _l.getAppkey(),
							Constant.INT_COUNT_TWO, _line.toString(), context);
				}

			}
			
		}
	}

	public static class ActivityUsersRetained1Reducer extends
			Reducer<IntKey, Text, Text, NullWritable> {
		private String dateTime = null;
		private long timeLong;
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		private static final Text result = new Text();
		protected void reduce(IntKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			JSONObject jsonActivity = null;//用户访问日志
			int i = 0;
			for (Text value : values) {
				if (key.getValue().get() == Constant.INT_COUNT_TWO && i == 0) {
					jsonActivity = JSONObject.parseObject(value.toString());
					i++;
				}
				
				if (key.getValue().get() == Constant.INT_COUNT_ONE) {
					
					JSONObject jsonMobile = JSONObject.parseObject(value.toString());
					String server_time = jsonMobile.getString(AppKey.server_time.toString()).substring(0, 8);
						
					if (dateTime.equals(server_time)) {
						jsonMobile.put("indicator", 0);
						result.set(jsonMobile.toJSONString());
						context.write(result, NullWritable.get());
					} else {
						if (jsonActivity != null) {
							if (jsonMobile.getString(AppKey.phone_udid.toString()).equals(jsonActivity.getString(AppKey.phone_udid.toString()))) {
								try {
									long tmpTime = sdf.parse(server_time).getTime();
									long extend_code = (this.timeLong - tmpTime) / 1000 / 3600 / 24;
									
									jsonMobile.put("indicator", extend_code);
									result.set(jsonMobile.toJSONString());
									context.write(result, NullWritable.get());
									
									
								} catch (ParseException e) {
								}

							}
						}
					}
				}
					
			
				
			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			this.dateTime = config.get(Constant.DATETIME);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			try {
				this.timeLong = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("/yyyyMMdd/");
		System.out.println((sdf.parse("/20140508/").getTime()-sdf.parse("/20140507/").getTime())/1000);
	}
}