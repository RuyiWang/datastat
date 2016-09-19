package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
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

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;

/**
 * 此MR已经废弃
 * @ClassName: ClientAscUsersMapReduce1
 * @Description: 计算升级用户数的第一个MR,这个MR主要计算APPKEY+UDID组合的升级情况。
 * @author 邓长春
 * @date 2012-9-12 下午02:19:56
 *
 */
public class ClientAscUsersMapReduce1 extends Configured implements Tool {
	public static final Log	LOG_MR	= LogFactory.getLog(ClientIndicatorMapReduce.class);
	public static String	ONE		= "1";

	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();

		FileInputFormat.setInputPaths(job, op.getPaths());
		// FileInputFormat.setInputPaths(job, new Path("/user/gauss/client/test"));
		String outPath = "/runtime/tmp/client/" + CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/") + java.util.UUID.randomUUID().toString();
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setMapperClass(ClientAscUsersMapper1.class);
		job.setReducerClass(ClientAscUsersReducer1.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			op.execute(args[0], outPath);
		}

		return 0;

		// return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ClientAscUsersMapper1 extends Mapper<LongWritable, Text, TextKey, Text> {

		@SuppressWarnings("unused")
		protected void map(LongWritable l, Text line, Context context) throws IOException, InterruptedException {
			AppLogFormat _l = new AppLogFormat(line.toString());
			if (null == _l) {
				return;
			}
			outputKeyValue(_l, context, ONE);

		}

		protected void outputKeyValue(AppLogFormat log, Context context, String v_str) throws IOException, InterruptedException {
			String appkey = log.getAppkey();
			if (StringUtils.isEmpty(appkey)) {
				appkey = Constant.OTHER_APP;
			}
			String version = log.getPhoneSoftversion();
			if (StringUtils.isEmpty(version)) {
				version = Constant.OTHER_VERSION;
			}
			MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + log.getOpenudid(), version, v_str, context);

		}

	}

	public static class ClientAscUsersReducer1 extends Reducer<TextKey, Text, Text, NullWritable> {
		private String	dateTime;
		private String	jobId;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.jobId = context.getJobID().toString();
		}

		protected void reduce(TextKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keyArr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appkey = keyArr[0];
			String openudid = keyArr[1];
			String version = key.getValue().toString();
			Map<String, String> map = new HashMap<String, String>();
			// 此处二次排序是降序
			for (Text v : values) {
				if (!version.equals(key.getValue().toString())) {
					map.put(Constant.DATETIME, dateTime);
					map.put(Constant.JOBID, jobId);
					map.put(Constant.APPKEY, appkey);
					map.put(Constant.OPENUDID, openudid);
					// 一个用户从版本A升级到版本B，则此刻讨论这个升级用户是相对于版本B而言的。
					map.put(Constant.PHONESOFTVERSION, version);
					context.write(new Text(Json.toJson(map, Constant.JSON_FORMAT)), NullWritable.get());
				}
				version = key.getValue().toString();
			}
		}
	}

}
