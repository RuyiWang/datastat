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
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.LongKeyAsc;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;

/**
 * 
 * @ClassName: UsersIndicatorMapReduce1.java
 * @Description: 用户趋势分析--新增用户指标MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:37:37
 * 
 */
public class UsersIndicatorMapReduce1 extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(UsersIndicatorMapReduce1.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "true"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map), true);
		Job job = op.getJob();
		
		Configuration config  = job.getConfiguration();
		String granularity =  config.get(Constant.GRANULARITY);
		String path = "/runtime/tmp/client/"
				+ CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/")
				+ java.util.UUID.randomUUID().toString();
		if("day".equals(granularity.toLowerCase())){
			path = "/applog/tmp/tm/"+granularity.toLowerCase()+"/"+config.get(Constant.DATETIME);
		}
		
		FileInputFormat.setInputPaths(job, op.getPaths());
		FileOutputFormat.setOutputPath(job, new Path(path));

		job.setMapperClass(UsersIndicator1Mapper.class);
		job.setReducerClass(UsersIndicator1Reducer.class);

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
			map.put(Constant.APP_TMP_PATH, path);
			map.put("mapReduceClassName", "UsersIndicatorMapReduce2");
			ToolRunner.run(new Configuration(), new UsersIndicatorMapReduce2(), new String[]{Json.toJson(map)});
			if("day".equals(granularity.toLowerCase())){
				map.remove(Constant.APP_TMP_PATH);
				map.put("logTypes", "TM,A");
				map.put("configFile", "parameters_activity_users_retained.xml");
				map.put("mapReduceClassName", "ActivityUsersRetainedMapReduce1");
				ToolRunner.run(new Configuration(), new ActivityUsersRetainedMapReduce1(), new String[]{Json.toJson(map)});
			}
		 }
		 return 0;

	}

	public static class UsersIndicator1Mapper extends
			Mapper<LongWritable, Text, LongKeyAsc, Text> {
		private long time = 0L;
		private String granularity = null;

		private final SimpleDateFormat sdf_mon = new SimpleDateFormat("yyyyMM");
		private final SimpleDateFormat sdf_day = new SimpleDateFormat("yyyyMMdd");
		private final SimpleDateFormat sdf_hour = new SimpleDateFormat("yyyyMMddHH");
		private final Calendar calendar = Calendar.getInstance();

		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			JSONObject json = null;
			try {
				json = JSONObject.parseObject(_line.toString());
			} catch (Exception e) {
				return;
			}
			if (json == null)
				return;

			String phone_udid = json.getString("phone_udid");
			String appkey = json.getString("appkey");
			String server_time = json.getString("server_time");
			long timeTmp = 0L;
			if (StringUtils.isBlank(server_time) || StringUtils.isBlank(phone_udid)) {
				return;
			} else {
				try {

					if (Granularity.HOUR.toString().equals(granularity)
							|| Granularity.AHOUR.toString().equals(granularity)) {
						calendar.setTime(sdf_hour.parse(server_time));

					} else if (Granularity.DAY.toString().equals(granularity)
							|| Granularity.DAY7.toString().equals(granularity)) {
						server_time = server_time.substring(0, 8);
						calendar.setTime(sdf_day.parse(server_time));
					} else {
						server_time = server_time.substring(0, 6);
						calendar.setTime(sdf_mon.parse(server_time));
					}

				} catch (ParseException e) {
					e.printStackTrace();
				}
				timeTmp = calendar.getTimeInMillis();
				if (timeTmp <= time) {
					json.put("server_time", server_time);
					MapReduceHelpler.outputLongKeyValueAsc(appkey + Constant.DELIMITER + phone_udid, timeTmp, json.toJSONString(), context);
				}
			}

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration config = context.getConfiguration();
			String dateTime = config.get(Constant.DATETIME);

			this.granularity = config.get(Constant.GRANULARITY);
			try {
				SimpleDateFormat sdf = null;
				if (Granularity.HOUR.toString().equals(granularity)
						|| Granularity.AHOUR.toString().equals(granularity)) {
					sdf = new SimpleDateFormat("yyyyMMddHH");
				} else if (Granularity.DAY.toString().equals(granularity)
						|| Granularity.DAY7.toString().equals(granularity)) {
					sdf = new SimpleDateFormat("yyyyMMdd");
				} else {
					sdf = new SimpleDateFormat("yyyyMM");
				}
				this.time = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				LOG_MR.error(e);
			}
		}

	}

	public static class UsersIndicator1Reducer extends
			Reducer<LongKeyAsc, Text, Text, NullWritable> {
		private String granularity = null;
		private final Text result = new Text();
		private String dateTime = null;

		@Override
		protected void reduce(LongKeyAsc key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String word = "";
			String reslutLine = "";
			int count = 0;
			for (Text value : values) {
				JSONObject jsb = JSONObject.parseObject(value.toString());
				String info = jsb.getString("phone_udid") + jsb.getString("server_time");
				if (!word.equals(info)) {
					word = info;
					reslutLine = value.toString();
					count++;
				}
			}

			if (count == 1) {
				JSONObject jsb = JSONObject.parseObject(reslutLine);
				String server_time = jsb.getString("server_time");
				if (this.dateTime.equals(server_time)) {
					result.set(reslutLine);
					context.write(result, NullWritable.get());
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration config = context.getConfiguration();
			this.granularity = config.get(Constant.GRANULARITY);
			this.dateTime = config.get(Constant.DATETIME);
		}
	}

}