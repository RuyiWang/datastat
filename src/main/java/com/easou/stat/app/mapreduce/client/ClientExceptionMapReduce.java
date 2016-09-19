package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
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
import com.easou.stat.app.constant.Dimensions;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.ExceptionInfo;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.mrDBEntity.ExceptionReducerCollectorDefine;
import com.easou.stat.app.util.MD5Util;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ClientExceptionMapReduce.java
 * @Description: 此MR没有用到
 * @author: 
 * @date: 2014年6月5日 上午10:58:39
 *
 */
public class ClientExceptionMapReduce extends Configured implements Tool {

	public static final Log LOG_MR = LogFactory
			.getLog(ClientExceptionMapReduce.class);

	public static class ClientExceptionMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private boolean dim_type_version = Boolean.FALSE;
		public static IntWritable ONE = new IntWritable(1);

		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();

			List<Dimensions> dimTypes = MRDriver.loadDimTypes(conf);
			// 加载维度类型
			this.dim_type_version = dimTypes.contains(Dimensions.version);
		}

		protected void map(LongWritable l, Text line, Context context)
				throws IOException, InterruptedException {
			AppLogFormat _l = new AppLogFormat(line.toString());

			outputKeyValue(_l, context);

		}

		protected void outputKeyValue(AppLogFormat log, Context context)
				throws IOException, InterruptedException {
			String appkey = log.getAppkey();

			if (StringUtils.isEmpty(appkey)) {
				appkey = Constant.OTHER_APP;
			}

			List<ExceptionInfo> infos = log.getExceptions();

			if (null != infos && infos.size() > 0) {
				for (int i = 0; i < infos.size(); i++) {
					ExceptionInfo info = infos.get(i);
					String exception = info.getStack();
					String time = info.getStrTime();
					if (dim_type_version) {
						String version = StringUtils.isBlank(log
								.getExceptVersion()) ? Constant.OTHER_VERSION
								: log.getExceptVersion();
						// 对应的activity跳到其他activity的次数
						context.write(new Text(appkey + Constant.DELIMITER
								+ version + Constant.DELIMITER + exception
								+ Constant.DELIMITER + time), ONE);
					}
				}
			}

		}
	}

	public static class ClientExceptionReducer
			extends
			Reducer<Text, IntWritable, ExceptionReducerCollectorDefine, NullWritable> {
		private String dateTime;
		private String jobId;

		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String[] keyArr = key.toString().split(Constant.DELIMITER_REG);

			String appkey = keyArr[0];
			String version = keyArr[1];
			String exception = keyArr[2];
			String time = keyArr[3];
			int result = 0;

			for (IntWritable v : values) {
				result++;
			}

			Float f = Float.parseFloat(result + "");
			String md5result = MD5Util.MD5(exception);
			String dim_type = "version";
			System.out.println(jobId + "===" + dateTime + "===" + appkey
					+ "===" + md5result + "===" + dim_type + "===" + exception
					+ "===" + time + "===" + 7 + "===" + f + "===" + 0 + "==="
					+ version + "===" + "-" + "===" + "-");
			ExceptionReducerCollectorDefine o = null;

			o = new ExceptionReducerCollectorDefine(jobId, dateTime, appkey,
					md5result, dim_type, exception, time, 7, f, 0, version,
					"-", "-");

			context.write(o, NullWritable.get());

		}

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.jobId = context.getJobID().toString();
		}
	}

	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);

		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ExceptionReducerCollectorDefine
				.getTableNameByGranularity(op.getGranularity()),
				ExceptionReducerCollectorDefine.getFields());

		job.setMapperClass(ClientExceptionMapper.class);
		job.setReducerClass(ClientExceptionReducer.class);

		// job.setPartitionerClass(FirstPartitioner.class);
		// job.setGroupingComparatorClass(Text.GroupingComparator.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(ExceptionReducerCollectorDefine.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(OracleDBOutputFormat.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
