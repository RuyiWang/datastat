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
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorClientUsersRetained;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;

/**
 * 
 * @ClassName: ClientUsersRetainedMapReduce.java
 * @Description: 此MR已经废弃
 * @author: Asa
 * @date: 2014年5月21日 上午9:57:27
 *
 */
public class ClientUsersRetainedMapReduce extends Configured implements Tool {
	public static final Log	LOG_MR	= LogFactory.getLog(ClientUsersRetainedMapReduce.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);

		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorClientUsersRetained.getTableNameByGranularity(op.getGranularity()), ReducerCollectorClientUsersRetained.getFields());

		job.setMapperClass(ClientUsersRetainedMapper.class);
		job.setReducerClass(ClientUsersRetainedReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ReducerCollectorClientUsersRetained.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(OracleDBOutputFormat.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ClientUsersRetainedMapper extends Mapper<LongWritable, Text, TextKey, Text> {
		// 纬度
		private boolean	dim_type_city				= Boolean.FALSE;
		private boolean	dim_type_cpid				= Boolean.FALSE;
		private boolean	dim_type_version			= Boolean.FALSE;
		private boolean	dim_type_phone_model		= Boolean.FALSE;
		private boolean	dim_type_phone_apn			= Boolean.FALSE;
		private boolean	dim_type_phone_resolution	= Boolean.FALSE;
		private boolean	dim_type_os					= Boolean.FALSE;
		private boolean	dim_type_os_and_cpid		= Boolean.FALSE;

		private String	dateTime;
		private String	granularity;
		private int		time_period;									// 时间间隔
		private String	activeTime;									// 激活时间
		private String	k_str;											// 激活时间{]时间间隔(时间间隔范围为[0,8])

		@SuppressWarnings("unused")
		@Override
		protected void map(LongWritable l, Text _line, Context context) throws IOException, InterruptedException {
			AppLogFormat _l = new AppLogFormat(_line.toString());
			if (_l == null) {
				return;
			}
			activeTime = _l.getStrTime().substring(0, 8);
			// 天
			if (Granularity.DAY.toString().equals(granularity)) {
				time_period = CommonUtils.getDays(activeTime, dateTime);
				if (time_period >= 0 && time_period < 9) {
					k_str = activeTime + Constant.DELIMITER + time_period;
					outputKeyValue(_l, k_str, context);
					outputKeyValueForApnAnalyse(_l, k_str, context);
				}
			}
			// 周
			if (Granularity.WEEK.toString().equals(granularity)) {
				time_period = CommonUtils.getWeeks(activeTime, dateTime);
				if (time_period >= 0 && time_period < 9) {
					k_str = CommonUtils.getDayOfWeek(activeTime) + Constant.DELIMITER + time_period;
					outputKeyValue(_l, k_str, context);
					outputKeyValueForApnAnalyse(_l, k_str, context);
				}
			}
			// 月
			if (Granularity.MONTH.toString().equals(granularity)) {
				activeTime = activeTime.substring(0, 6);
				time_period = CommonUtils.getMonths(activeTime, dateTime);
				if (time_period >= 0 && time_period < 9) {
					k_str = activeTime + Constant.DELIMITER + time_period;
					outputKeyValue(_l, k_str, context);
					outputKeyValueForApnAnalyse(_l, k_str, context);
				}
			}
		}

		private void outputKeyValue(AppLogFormat _l, String k_str, Context context) throws IOException, InterruptedException {
			String appkey = _l.getAppkey();
			String udid = _l.getOpenudid();
			if (StringUtils.isNotBlank(appkey) && StringUtils.isNotBlank(udid)) {
				if (dim_type_city) {
					String city = StringUtils.isBlank(_l.getProvince()) ? Constant.OTHER_ZONE : _l.getProvince();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + city + Constant.DELIMITER + k_str, udid, Dimensions.city.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + city + Constant.DELIMITER + k_str, udid, Dimensions.city.toString(), context);
				}

				if (dim_type_cpid) {
					String cpidAll = _l.getSalechannel1All();
					String cpid1 = _l.getSalechannel1();
					String cpid2 = _l.getSalechannel2();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + cpidAll + Constant.DELIMITER + k_str, udid, Dimensions.cpid.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + cpidAll + Constant.DELIMITER + k_str, udid, Dimensions.cpid.toString(), context);

					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + cpid1 + Constant.DELIMITER + k_str, udid, Dimensions.cpid.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + cpid1 + Constant.DELIMITER + k_str, udid, Dimensions.cpid.toString(), context);

					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + cpid2 + Constant.DELIMITER + k_str, udid, Dimensions.cpid.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + cpid2 + Constant.DELIMITER + k_str, udid, Dimensions.cpid.toString(), context);
				}

				if (dim_type_version) {
					String version = StringUtils.isBlank(_l.getPhoneSoftversion()) ? Constant.OTHER_VERSION : _l.getPhoneSoftversion();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + version + Constant.DELIMITER + k_str, udid, Dimensions.version.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + version + Constant.DELIMITER + k_str, udid, Dimensions.version.toString(), context);
				}

				if (dim_type_phone_model) {
					String model = StringUtils.isBlank(_l.getPhoneModel()) ? Constant.OTHER_MODEL : _l.getPhoneModel();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + model + Constant.DELIMITER + k_str, udid, Dimensions.phone_model.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + model + Constant.DELIMITER + k_str, udid, Dimensions.phone_model.toString(), context);
				}

				if (dim_type_phone_apn) {
					String apn = StringUtils.isBlank(_l.getPhoneApn()) ? Constant.OTHER_APN : _l.getPhoneApn();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + apn + Constant.DELIMITER + k_str, udid, Dimensions.phone_apn.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + apn + Constant.DELIMITER + k_str, udid, Dimensions.phone_apn.toString(), context);
				}

				if (dim_type_phone_resolution) {
					String resolution = StringUtils.isBlank(_l.getPhoneResolution()) ? Constant.OTHER_RESOLUTION : _l.getPhoneResolution();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + resolution + Constant.DELIMITER + k_str, udid, Dimensions.phone_resolution.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + resolution + Constant.DELIMITER + k_str, udid, Dimensions.phone_resolution.toString(), context);
				}

				if (dim_type_os) {
					String os = StringUtils.isBlank(_l.getOs()) ? Constant.OTHER_OS : _l.getOs();
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + os + Constant.DELIMITER + k_str, udid, Dimensions.os.toString(), context);
					MapReduceHelpler.outputKeyValue(Constant.ALLAPP + Constant.DELIMITER + os + Constant.DELIMITER + k_str, udid, Dimensions.os.toString(), context);
				}
			}
		}

		protected void outputKeyValueForApnAnalyse(AppLogFormat _l, String k_str, Context context) throws IOException, InterruptedException {
			String appkey = StringUtils.isBlank(_l.getAppkey()) ? Constant.OTHER_APP : _l.getAppkey();
			if (dim_type_os_and_cpid) {
				String os = StringUtils.isBlank(_l.getOs()) ? Constant.OTHER_OS : _l.getOs();
				// 一级所有
				String cpid = _l.getSalechannel1All();
				MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + os + Constant.DELIMITER + cpid + Constant.DELIMITER + k_str, _l.getOpenudid(), Dimensions.osAndCpid.toString(), context);
				// 一级渠道
				cpid = _l.getSalechannel1();
				MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + os + Constant.DELIMITER + cpid + Constant.DELIMITER + k_str, _l.getOpenudid(), Dimensions.osAndCpid.toString(), context);
				// 二级渠道
				cpid = _l.getSalechannel2();
				MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + os + Constant.DELIMITER + cpid + Constant.DELIMITER + k_str, _l.getOpenudid(), Dimensions.osAndCpid.toString(), context);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.granularity = context.getConfiguration().get(Constant.GRANULARITY);

			Configuration conf = context.getConfiguration();
			// 加载维度类型
			List<Dimensions> dimTypes = MRDriver.loadDimTypes(conf);
			this.dim_type_city = dimTypes.contains(Dimensions.city);
			this.dim_type_cpid = dimTypes.contains(Dimensions.cpid);
			this.dim_type_version = dimTypes.contains(Dimensions.version);
			this.dim_type_phone_model = dimTypes.contains(Dimensions.phone_model);
			this.dim_type_phone_apn = dimTypes.contains(Dimensions.phone_apn);
			this.dim_type_phone_resolution = dimTypes.contains(Dimensions.phone_resolution);
			this.dim_type_os = dimTypes.contains(Dimensions.os);
			this.dim_type_os_and_cpid = dimTypes.contains(Dimensions.osAndCpid);
		}
	}

	public static class ClientUsersRetainedReducer extends Reducer<TextKey, Text, ReducerCollectorClientUsersRetained, NullWritable> {

		protected void reduce(TextKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String udid = null;
			Float numberOfUsersRetained = 0f;
			String dimType = "";
			String[] keyArr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appkey = keyArr[0];
			String dimensions_code = "";
			String extension_code = "-";
			String time = "";
			int time_period = 0;
			int keyLen = keyArr.length;
			// appkey{]dim{]激活时间{]时间间隔
			if (keyLen == 4) {
				dimensions_code = keyArr[1];
				time = keyArr[2];
				time_period = Integer.parseInt(keyArr[3]);

			}
			// appkey{]os{]cpid{]激活时间{]时间间隔
			else if (keyLen == 5) {
				dimensions_code = keyArr[1];
				extension_code = keyArr[2];
				time = keyArr[3];
				time_period = Integer.parseInt(keyArr[4]);
			}
			// 用户去重
			for (Text v : values) {
				if (!key.getValue().toString().equals(udid)) {
					numberOfUsersRetained++;
				}
				udid = key.getValue().toString();
				dimType = v.toString();
			}

			// System.out.println("key.getDimension().toString()=" + key.getDimension().toString());
			ReducerCollectorClientUsersRetained r = new ReducerCollectorClientUsersRetained();
			r.setJobid(context.getJobID().toString());
			r.setTime(time);
			r.setDim_type(dimType);
			r.setAppkey(appkey);
			r.setDimensions_code(dimensions_code);
			r.setExtension_code(extension_code);
			r.setTime_period(time_period);
			r.setNumber_users(numberOfUsersRetained);
			context.write(r, NullWritable.get());
		}
	}
}
