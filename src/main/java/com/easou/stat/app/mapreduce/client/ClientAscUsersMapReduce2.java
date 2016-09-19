package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Dimensions;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorClient;
import com.easou.stat.app.util.MRDriver;

/**
 * 此MR已经废弃
 * @ClassName: ClientAscUsersMapReduce2
 * @Description: 计算升级用户数的第二个MR,这个MR主要计算APPKEY+PHONESOFTVERSION组合的升级情况,统计最终的用户数。
 * @author 邓长春
 * @date 2012-9-12 下午02:20:57
 * 
 */
public class ClientAscUsersMapReduce2 extends Configured implements Tool {
	public static final Log	LOG_MR	= LogFactory.getLog(ClientIndicatorMapReduce.class);
	public static String	ONE		= "1";

	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String, String> map = Json.fromJson(Map.class, args[0]);

		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();

		FileInputFormat.setInputPaths(job, op.getPaths());
		// FileInputFormat.setInputPaths(job, new Path("/user/gauss/client/asc/20120912/1403/d31f6155-73c9-434f-9736-17e9e09cb8d2"));
		DBOutputFormat.setOutput(job, ReducerCollectorClient.getTableNameByGranularity(op.getGranularity()), ReducerCollectorClient.getFields());

		job.setMapperClass(ClientAscUsersMapper2.class);
		job.setReducerClass(ClientAscUsersReducer2.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ReducerCollectorClient.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(OracleDBOutputFormat.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ClientAscUsersMapper2 extends Mapper<LongWritable, Text, TextKey, Text> {

		@SuppressWarnings("unused")
		protected void map(LongWritable l, Text line, Context context) throws IOException, InterruptedException {
			Map<String, String> map = Json.fromJson(Map.class, line.toString());
			String appkey = map.get(Constant.APPKEY);
			String openudid = map.get(Constant.OPENUDID);
			String version = map.get(Constant.PHONESOFTVERSION);
			MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + version, openudid, line.toString(), context);
		}

	}

	public static class ClientAscUsersReducer2 extends Reducer<TextKey, Text, ReducerCollectorClient, NullWritable> {

		protected void reduce(TextKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keyArr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appkey = keyArr[0];
			String version = keyArr[1];

			long numberAscUsers = 0L;
			String openudid = "";
			Map<String, String> map = new HashMap<String, String>();
			for (Text v : values) {
				if (!openudid.equals(key.getValue().toString())) {
					numberAscUsers++;
				}
				openudid = key.getValue().toString();
				map = Json.fromJson(Map.class, v.toString());
			}
			ReducerCollectorClient o = new ReducerCollectorClient(map.get(Constant.JOBID), map.get(Constant.DATETIME), Dimensions.version.toString(), appkey, version, "-", "-", Constant.NUMBER_ASC_USERS,
					Float.valueOf(numberAscUsers + ""));
			context.write(o, NullWritable.get());
		}
	}

}
