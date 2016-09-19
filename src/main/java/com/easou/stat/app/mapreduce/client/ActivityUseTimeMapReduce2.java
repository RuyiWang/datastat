package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.ActivityDimensions;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorUseTime;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityUseTimeMapReduce2.java
 * @Description: 用户使用时长分析MR step-2
 * @author: Asa
 * @date: 2014年5月13日 下午4:35:42
 *
 */
public class ActivityUseTimeMapReduce2 extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityUseTimeMapReduce2.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "false"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "false"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map), false);

		Job job = op.getJob();
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorUseTime.getTableNameByGranularity(op.getGranularity()),ReducerCollectorUseTime.getFields());

		job.setMapperClass(ActivityUseTime2Mapper.class);
		job.setReducerClass(ActivityUseTime2Reducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setOutputKeyClass(ReducerCollectorUseTime.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		return (job.waitForCompletion(true) ? 0 : 1);

	}

	public static class ActivityUseTime2Mapper extends
			Mapper<LongWritable, Text, TextKey, Text> {

		private long time = 0L;
		private String granularity = null;

		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString()))
				return;

			String dateFormat = "yyyyMMddHHmmss";
			Date date = null;
			String peroid = "0";
			if (Granularity.HOUR.toString().equals(granularity)) {
				date = DateUtil.getFirstMinuteOfHour(time);
			} else if (Granularity.DAY.toString().equals(granularity)) {
				date = DateUtil.getFirstHourOfDay(time);
				peroid = "1";
			} else if (Granularity.WEEK.toString().equals(granularity)) {
				date = DateUtil.getMondayOfWeek(time);
				peroid = "7";
			} else {
				date = DateUtil.getFirstDayOfMonth(time);
				peroid = "30";
			}
			String dateTime = CommonUtils.getDateTime(date.getTime(),
					dateFormat);

			JSONObject jsonObj = JSONObject.parseObject(_line.toString());
			String appkey = jsonObj.getString("appkey");
			String phone_softversion = jsonObj.getString("phone_softversion");
			String phone_esid = jsonObj.getString("phone_esid");
			String cpid = jsonObj.getString("cpid");
			double useTime = jsonObj.getDoubleValue("usetime");

			String useTimeRange = getUseTimeRange(useTime);

			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, phone_softversion, true);
			dims = composeDims(dims, dateTime, false);
			dims = composeDims(dims, peroid, false);
			dims = composeDims(dims, useTimeRange, false);
			List<String> cpidDims = composeDims(dims, "cpid", cpid, true);
			writeByDim(cpidDims, phone_esid, context);
		}

		private void writeByDim(List<String> dims, String phone_esid,
				Context context) throws IOException, InterruptedException {

			for (String dim : dims) {
				Text textValue = new Text(phone_esid);
				TextKey textKey = new TextKey(new Text(dim + Constant.DELIMITER
						+ ActivityDimensions.starttimes.toString()), new Text(
						phone_esid));
				context.write(textKey, textValue);
			}

		}

		private List<String> composeDims(List<String> prefixs, String dim,
				boolean isAll) {
			List<String> list = new ArrayList<String>();
			if (prefixs == null || prefixs.size() == 0) {
				list.add(dim);
				if (isAll)
					list.add("all");
			} else {
				for (String prefix : prefixs) {
					list.add(prefix + Constant.DELIMITER + dim);
					if (isAll)
						list.add(prefix + Constant.DELIMITER + "all");
				}
			}
			return list;
		}

		private List<String> composeDims(List<String> prefixs, String dim,
				String dimCode, boolean isAll) {
			List<String> list = new ArrayList<String>();
			dimCode = StringUtils.trimToNull(dimCode);
			if (prefixs == null || prefixs.size() == 0) {
				list.add(dim + Constant.DELIMITER + dimCode);
				if (isAll)
					list.add("all");
			} else {
				for (String prefix : prefixs) {
					if (StringUtils.isNotEmpty(dimCode))
						list.add(prefix + Constant.DELIMITER + dim
								+ Constant.DELIMITER + dimCode);
					if (isAll)
						list.add(prefix + Constant.DELIMITER + dim
								+ Constant.DELIMITER + "all");
				}
			}
			return list;
		}

		private String getUseTimeRange(double useTime) {
			if (useTime <= 3) {
				return "1";
			} else if (useTime <= 10) {
				return "2";
			} else if (useTime <= 30) {
				return "3";
			} else if (useTime <= 59) {
				return "4";
			} else if (useTime <= 179) {
				return "5";
			} else if (useTime <= 599) {
				return "6";
			} else {
				return "7";
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration config = context.getConfiguration();
			this.granularity = config.get(Constant.GRANULARITY);
			String dateTime = config.get(Constant._DATETIME);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			try {
				this.time = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

	}

	public static class ActivityUseTime2Reducer extends
			Reducer<TextKey, Text, ReducerCollectorUseTime, NullWritable> {
		private String jobId = null;

		@Override
		protected void reduce(TextKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String[] keyStr = key.getDimension().toString()
					.split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String phone_softversion = keyStr[1];
			String dateTime = keyStr[2];
			String time_peroid = keyStr[3];
			String extend_dimcode = keyStr[4];
			String cpid = keyStr[5];
			String dim_code = keyStr[6];
			String indicator = keyStr[7];
			ReducerCollectorUseTime outData = new ReducerCollectorUseTime();
			outData.setJobid(jobId);
			outData.setAppkey(appKey);
			outData.setPhone_softversion(phone_softversion);
			outData.setStat_date(dateTime);
			outData.setTime_peroid(time_peroid);
			outData.setExtend_dimcode(extend_dimcode);
			outData.setDim_type(cpid);
			outData.setDim_code(dim_code);
			outData.setIndicator(indicator);

			if (ActivityDimensions.starttimes.toString().equalsIgnoreCase(
					indicator)) {
				int count = 0;
				for (Text value : values) {
					count++;
				}
				outData.setValue((float) count);
				context.write(outData, NullWritable.get());
			}

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.jobId = context.getJobID().toString();
		}

	}

}
