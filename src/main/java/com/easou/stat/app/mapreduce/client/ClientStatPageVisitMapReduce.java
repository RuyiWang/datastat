package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import com.easou.stat.app.mapreduce.core.Activity;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.mrDBEntity.PageVisitReducerCollectorDefine;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ClientStatPageVisitMapReduce.java
 * @Description: 此MR已经废弃
 * @author: Asa
 * @date: 2014年6月5日 上午11:00:39
 *
 */
public class ClientStatPageVisitMapReduce extends Configured implements Tool {

	public static String ONE = "1";
	public static final Log LOG_MR = LogFactory
			.getLog(ClientStatPageVisitMapReduce.class);

	public static class ClientStatPageVisitMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private boolean dim_type_version = Boolean.FALSE;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();

			List<Dimensions> dimTypes = MRDriver.loadDimTypes(conf);// 加载维度类型
			this.dim_type_version = dimTypes.contains(Dimensions.version);
		}

		protected void map(LongWritable l, Text line, Context context)
				throws IOException, InterruptedException {
			AppLogFormat _l = new AppLogFormat(line.toString());
			if (null == _l) {
				return;
			}
			outputKeyValue(_l, context, ONE);

		}

		protected void outputKeyValue(AppLogFormat log, Context context,
				String v_str) throws IOException, InterruptedException {
			String appkey = log.getAppkey();
			// String phoneESID = log.getPhoneApn();

			List<Activity> activitys = null;
			activitys = log.getActivitys();

			if (null != activitys && activitys.size() > 0) {
				for (int i = 0; i < activitys.size(); i++) {
					Activity activity = activitys.get(i);
					String referActivity = activity.getReferActivity();
					String currentActivity = activity.getCurrentActivity();
					System.out.println(referActivity + "###" + currentActivity);
					// log.get

					if (StringUtils.isEmpty(appkey)) {
						appkey = Constant.OTHER_APP;
					}

					if (dim_type_version) {
						String version = StringUtils.isBlank(log
								.getPhoneSoftversion()) ? Constant.OTHER_VERSION
								: log.getPhoneSoftversion();
						// 对应的activity跳到其他activity的次数
						context.write(new Text(appkey + Constant.DELIMITER
								+ referActivity + Constant.DELIMITER
								+ currentActivity + Constant.DELIMITER
								+ version), new Text(ONE));

						// System.out.println("====================1===" +
						// appkey
						// + Constant.DELIMITER + referActivity
						// + Constant.DELIMITER + "" + Constant.DELIMITER
						// + version);

//						// 对应的activity离开应用的次数
//						context.write(new Text(appkey + Constant.DELIMITER
//								+ referActivity + Constant.DELIMITER + ""
//								+ Constant.DELIMITER + version), new Text(ONE));

						// // 对应的activity点击的总次数
						// context.write(new Text("all_activity_click_nums"),
						// new Text(ONE));
					}
				}

			}
		}
	}

	public static class ClientStatPageVisitReducer extends
			Reducer<Text, Text, PageVisitReducerCollectorDefine, NullWritable> {
		private String dateTime;
		private String jobId;

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// System.out.println("=================3==="+key.toString());
			String[] keyArr = key.toString().split(Constant.DELIMITER_REG);

			String target = "-";
			String des = "-";
			String appkey = "-";
			String version = "-";

			int result = 0;

			for (Text v : values) {
				result++;
			}

			Float f = Float.parseFloat(result + "");

			if (keyArr != null && keyArr.length > 1) {
				target = keyArr[2];
				des = keyArr[1];
				appkey = keyArr[0];
				version = keyArr[3];
			}

			String dim_type = "";
			// String version = keyAtrr[];
			if (keyArr != null && keyArr.length > 1) {
				if (keyArr.length == 4) {
					dim_type = "version";
				} else {
					dim_type = "all_activity_click_nums";
				}
			}

			// 把结果存放到数据库,indicator=6表示点击次数，indicator=9是累积用户数
			PageVisitReducerCollectorDefine o = null;
			System.out.println("====================2===" + jobId + "######"
					+ dateTime + "######" + appkey + "######" + dim_type
					+ "######" + des + "######" + target + "######" + 6
					+ "######" + f + "######" + version + "######" + "-"
					+ "######" + "-");
			o = new PageVisitReducerCollectorDefine(jobId, dateTime, appkey,
					dim_type, des, target, 6, f, version, "-", "-");

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
		DBOutputFormat.setOutput(job, PageVisitReducerCollectorDefine
				.getTableNameByGranularity(op.getGranularity()),
				PageVisitReducerCollectorDefine.getFields());

		job.setMapperClass(ClientStatPageVisitMapper.class);
		job.setReducerClass(ClientStatPageVisitReducer.class);

		// job.setPartitionerClass(FirstPartitioner.class);
		// job.setGroupingComparatorClass(Text.GroupingComparator.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(PageVisitReducerCollectorDefine.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(OracleDBOutputFormat.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
