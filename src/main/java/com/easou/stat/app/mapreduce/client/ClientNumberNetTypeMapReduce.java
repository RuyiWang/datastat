package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorClient;
import com.easou.stat.app.util.MRDriver;

/**
 * 此MR已经废弃
 * @ClassName: ClientNumberNetTypeMapReduce
 * @Description: 计算上网方式用户占比的MR
 * @author 邓长春
 * @date 2012-9-17 下午05:33:29
 * 
 */
public class ClientNumberNetTypeMapReduce extends Configured implements Tool {
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
		// FileInputFormat.setInputPaths(job, new Path("/user/gauss/client/test"));
		DBOutputFormat.setOutput(job, ReducerCollectorClient.getTableNameByGranularity(op.getGranularity()), ReducerCollectorClient.getFields());

		job.setMapperClass(ClientNumberNetTypeMapper.class);
		job.setReducerClass(ClientNumberNetTypeReducer.class);

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

	public static class ClientNumberNetTypeMapper extends Mapper<LongWritable, Text, TextKey, Text> {

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
			String udid = log.getOpenudid();
			String apn = log.getPhoneApn();
			if (StringUtils.isEmpty(apn)) {
				apn = Constant.OTHER_APN;
			}
			String netType = log.getCurrentnetWorktype();
			netType = processNetType(netType);
			// context.write(new Text(appkey + Constant.DELIMITER + netType), new Text(ONE));
			// context.write(new Text(appkey + Constant.DELIMITER + apn + netType), new Text(ONE));
			MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + netType, udid, ONE, context);
			MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + apn + netType, udid, ONE, context);

		}

		private String processNetType(String netType) {
			String netTypeOut = "";
			if (StringUtils.isEmpty(netType)) {
				netTypeOut = Constant.OTHER_NETTYPE;
			} else {
				if ("wifi".toLowerCase().equals(netType.toLowerCase())) {
					netTypeOut = "wifi";
				} else if ("wap".toLowerCase().equals(netType.toLowerCase()) || "mobile".toLowerCase().equals(netType.toLowerCase())) {
					netTypeOut = "wap";

				} else if ("net".toLowerCase().equals(netType.toLowerCase())) {
					netTypeOut = "net";
				} else {
					netTypeOut = "other";
				}
			}
			return netTypeOut;
		}

	}

	public static class ClientNumberNetTypeReducer extends Reducer<TextKey, Text, ReducerCollectorClient, NullWritable> {
		private String	dateTime;
		private String	jobId;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.jobId = context.getJobID().toString();
		}

		protected void reduce(TextKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keyArr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String curUDID = "";// 当前用户的UDID
			long numberNetType = 0L;
			for (Text v : values) {
				if (!curUDID.trim().equals(key.getValue().toString())) {
					numberNetType++;
				}
				curUDID = key.getValue().toString();
			}
			// 开始写库
			ReducerCollectorClient o = null;
			o = new ReducerCollectorClient(jobId, dateTime, Dimensions.currentnetworktype.toString(), keyArr[0], keyArr[1], "-", "-", Constant.NUMBER_NET_TYPE, Float.valueOf(numberNetType + ""));
			context.write(o, NullWritable.get());
		}
	}

}
