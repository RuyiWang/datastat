package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
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

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.IntKey;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityMapReduce.java
 * @Description: 终端分析MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:29:29
 *
 */
public class ActivityMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityMapReduce.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "true"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();
		String path = "/runtime/tmp/client/"
				+ CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/")
				+ java.util.UUID.randomUUID().toString();
		FileInputFormat.setInputPaths(job, op.getPaths());
		FileOutputFormat.setOutputPath(job, new Path(path));

		job.setMapperClass(ActivityMapper.class);
		job.setReducerClass(ActivityReducer.class);

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
			map.put("mapReduceClassName", "ClientTerminalIndicatorMapReduce");
			ToolRunner.run(new Configuration(), new ClientTerminalIndicatorMapReduce(), new String[]{Json.toJson(map)});
		}
		return 0;
	}

	public static class ActivityMapper extends
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
						Constant.INT_COUNT_TWO, _line.toString(), context);
			}else if (_l.isActivityInfo()) {
				MapReduceHelpler.outputIntKeyValue(
						_l.getOpenudid() + _l.getAppkey(),
						Constant.INT_COUNT_ONE, _line.toString(), context);
			}
		}
	}

	public static class ActivityReducer extends
			Reducer<IntKey, Text, Text, NullWritable> {

		@SuppressWarnings("unchecked")
		protected void reduce(IntKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Object> mapAValue = new HashMap<String, Object>();//用户访问日志
			Map<String, Object> map = new HashMap<String, Object>();
			// 处理客户端用户信息有多条的情况(只取一条)
			int i = 0;
			for (Text v : values) {
				if (key.getValue().get() == Constant.INT_COUNT_TWO && i == 0) {
					Map<String, Object> mapMValue = Json.fromJson(Map.class, v.toString());
					map.put("time", mapMValue.get("time"));
					map.put("phone_firmware_version", mapMValue.get("phone_firmware_version"));
					map.put("phone_model", mapMValue.get("phone_model"));
					map.put("phone_resolution", mapMValue.get("phone_resolution"));
					i++;
				}
				// 如果没有用户客户端日志，只有用户行为日志，把用户行为日志废除
				if (map.size() > 0) {
					if (key.getValue().get() == Constant.INT_COUNT_ONE) {
							mapAValue = Json.fromJson(Map.class, v.toString());
							mapAValue.putAll(map);
							context.write(new Text(Json.toJson(mapAValue,	Constant.JSON_FORMAT)), NullWritable.get());
					}
				}
			}
		}

	}
}
