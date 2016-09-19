package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.ArrayList;
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
import com.easou.stat.app.constant.Indicators;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.Behavior;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorClient;
import com.easou.stat.app.mapreduce.util.SurvivalTimeUnit;
import com.easou.stat.app.mapreduce.util.SurvivalTimeUtil;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;

/**
 * 此MR已经废弃
 * @ClassName: ClientIndicatorMapReduce
 * @Description: 统计客户端各项指标的MR
 * @author 邓长春
 * @date 2012-9-6 下午02:50:55
 * 
 */
public class ClientIndicatorMapReduce extends Configured implements Tool {
	public static final Log	LOG_MR	= LogFactory.getLog(ClientIndicatorMapReduce.class);
	public static String	ZERO	= "0";
	public static String	ONE		= "1";

	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String, String> map = Json.fromJson(Map.class, args[0]);

		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();

		// FileInputFormat.setInputPaths(job, op.getPaths());
		// FileInputFormat.setInputPaths(job, new Path("/user/gauss/client/test"));
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorClient.getTableNameByGranularity(op.getGranularity()), ReducerCollectorClient.getFields());

		job.setMapperClass(ClientIndicatorMapper.class);
		job.setReducerClass(ClientIndicatorReducer.class);

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

	public static class ClientIndicatorMapper extends Mapper<LongWritable, Text, TextKey, Text> {
		// 指标
		private boolean	number_users				= false;
		private boolean	number_new_users			= false;
		private boolean	number_active_users			= false;
		private boolean	times_start					= false;
		private boolean	time_average				= false;
		// 纬度
		private boolean	dim_type_city				= Boolean.FALSE;
		private boolean	dim_type_cpid				= Boolean.FALSE;
		private boolean	dim_type_version			= Boolean.FALSE;
		private boolean	dim_type_phone_model		= Boolean.FALSE;
		private boolean	dim_type_phone_apn			= Boolean.FALSE;
		private boolean	dim_type_phone_resolution	= Boolean.FALSE;
		private boolean	dim_type_os					= Boolean.FALSE;
		private boolean	dim_type_cpid_code			= Boolean.FALSE;
		private boolean	dim_type_all				= Boolean.FALSE;
		private boolean	dim_type_os_and_cpid		= Boolean.FALSE;

		private String	dateTime;
		private String	granularity;
		private int		time_period;									// 时间间隔
		private String	activeTime;									// 激活时间

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// 加载解析指标
			List<Indicators> indicators = MRDriver.loadIndicators(conf);
			this.number_users = indicators.contains(Indicators.NUMBER_USERS);
			this.number_new_users = indicators.contains(Indicators.NUMBER_NEW_USERS);
			this.number_active_users = indicators.contains(Indicators.NUMBER_ACITVE_USERS);
			this.times_start = indicators.contains(Indicators.TIMES_START);
			this.time_average = indicators.contains(Indicators.TIME_AVERAGE);
			// 加载维度类型
			List<Dimensions> dimTypes = MRDriver.loadDimTypes(conf);
			this.dim_type_city = dimTypes.contains(Dimensions.city);
			this.dim_type_cpid = dimTypes.contains(Dimensions.cpid);
			this.dim_type_version = dimTypes.contains(Dimensions.version);
			this.dim_type_phone_model = dimTypes.contains(Dimensions.phone_model);
			this.dim_type_phone_apn = dimTypes.contains(Dimensions.phone_apn);
			this.dim_type_phone_resolution = dimTypes.contains(Dimensions.phone_resolution);
			this.dim_type_os = dimTypes.contains(Dimensions.os);
			this.dim_type_cpid_code = dimTypes.contains(Dimensions.cpid_code);
			this.dim_type_all = dimTypes.contains(Dimensions.all);
			this.dim_type_os_and_cpid = dimTypes.contains(Dimensions.osAndCpid);

			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.granularity = context.getConfiguration().get(Constant.GRANULARITY);
		}

		/**
		 * @Title: isNewUserWithTime
		 * @Description: 判断是否是指定时间段的新增用户，以激活时间为准，酌情处理是否同意。（统一留存分析当天 的新用户与基础分析新增用户的口径。）
		 * @param granularity
		 * @param dateTime
		 * @param activeTime
		 * @return
		 * @author 邓长春
		 * @return boolean
		 * @throws
		 */
		private boolean isNewUserWithTime(String granularity, String dateTime, String activeTime) {
			// 时
			if (Granularity.HOUR.toString().equals(granularity)) {
				return true;
			}
			// 天
			if (Granularity.DAY.toString().equals(granularity)) {
				time_period = CommonUtils.getDays(activeTime, dateTime);
				if (time_period == 0) {
					return true;
				}
			}
			// 周
			if (Granularity.WEEK.toString().equals(granularity)) {
				time_period = CommonUtils.getWeeks(activeTime, dateTime);
				if (time_period == 0) {
					return true;
				}
			}
			// 月
			if (Granularity.MONTH.toString().equals(granularity)) {
				activeTime = activeTime.substring(0, 6);
				time_period = CommonUtils.getMonths(activeTime, dateTime);
				if (time_period == 0) {
					return true;
				}
			}
			return false;
		}

		@SuppressWarnings("unused")
		protected void map(LongWritable l, Text line, Context context) throws IOException, InterruptedException {
			AppLogFormat _l = new AppLogFormat(line.toString());
			if (null == _l) {
				return;
			}

			String udid = _l.getOpenudid();
			// 新增用户数
			activeTime = _l.getStrTime().substring(0, 8);
			boolean isNewUser = isNewUserWithTime(granularity, dateTime, activeTime);
			if (Granularity.HOUR.toString().equals(granularity)) {
				isNewUser = ONE.equals(_l.getPhoneFirstRun());
			}
			if (isNewUser && this.number_new_users) {
				outputKeyValue(_l, context, Constant.NUMBER_NEW_USERS, udid, ONE);
				outputKeyValueForApnAnalyse(_l, context, Constant.NUMBER_NEW_USERS, udid, ONE);
			}
			// 需要日志类型限制
			// if (LogTypes.B.equals(_l.getLogType())) {
			List<Behavior> behaviors = _l.getBehaviors();
			if (null != behaviors) {
				int i = 0;
				for (Behavior b : behaviors) {
					// 用户数-基于行为日志
					if (this.number_users) {
						outputKeyValue(_l, context, Constant.NUMBER_USERS, udid, ONE);
						outputKeyValueForApnAnalyse(_l, context, Constant.NUMBER_USERS, udid, ONE);
					}
					// 活跃用户数-基于行为日志:活跃用户数（新版）： { 统计时间内注册的（激活时间在统计时间内的），启动次数>1 || 在统计时间前注册的（激活时间在统计时间前的，在后面的过滤掉） } 对phone_udid去重
					boolean active = (ONE.equals(_l.getPhoneFirstRun()) && b.isAppStart()) || !ONE.equals(_l.getPhoneFirstRun());
					if (active && this.number_active_users) {
						outputKeyValue(_l, context, Constant.NUMBER_ACITVE_USERS, udid + (ONE.equals(_l.getPhoneFirstRun()) ? ONE : ZERO), ONE);
						outputKeyValueForApnAnalyse(_l, context, Constant.NUMBER_ACITVE_USERS, udid + (ONE.equals(_l.getPhoneFirstRun()) ? ONE : ZERO), ONE);
					}
					// 启动次数(激活算一次启动权重)
					boolean start = b.isAppStart() || (i == 0 && ONE.equals(_l.getPhoneFirstRun()));
					if (start && this.times_start) {
						outputKeyValue(_l, context, Constant.TIMES_START, udid, ONE);
						outputKeyValueForApnAnalyse(_l, context, Constant.TIMES_START, udid, ONE);
					}
					i++;
					// 平均使用时长
					if (this.time_average) {
						// 时间后面加启动标志位，到时要算相对时间段长
						outputKeyValue(_l, context, Constant.TIME_AVERAGE, b.getTime() + (b.isAppStart() ? ONE : ZERO), ONE);
						outputKeyValueForApnAnalyse(_l, context, Constant.TIME_AVERAGE, b.getTime() + (b.isAppStart() ? ONE : ZERO), ONE);
					}
				}
			}
			// }
		}

		protected void outputKeyValue(AppLogFormat log, Context context, String indicatorType, String keyValue, String v_str) throws IOException, InterruptedException {
			String appkey = log.getAppkey();
			String udid = log.getOpenudid();

			if (StringUtils.isNotBlank(appkey) && StringUtils.isNotBlank(udid)) {
				if (dim_type_city) {
					String city = StringUtils.isBlank(log.getProvince()) ? Constant.OTHER_ZONE : log.getProvince();
					MapReduceHelpler.outputKeyValue(Dimensions.city.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + city, indicatorType + keyValue, v_str, context);
					// 地区-所有
					MapReduceHelpler.outputKeyValue(Dimensions.city.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + Constant.ALLAPP, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_cpid) {
					String cpid = log.getSalechannel2();
					MapReduceHelpler.outputKeyValue(Dimensions.cpid.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + cpid, indicatorType + keyValue, v_str, context);
					cpid = log.getSalechannel1();
					MapReduceHelpler.outputKeyValue(Dimensions.cpid.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + cpid, indicatorType + keyValue, v_str, context);
					cpid = log.getSalechannel1All();
					MapReduceHelpler.outputKeyValue(Dimensions.cpid.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + cpid, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_version) {
					String version = StringUtils.isBlank(log.getPhoneSoftversion()) ? Constant.OTHER_VERSION : log.getPhoneSoftversion();
					MapReduceHelpler.outputKeyValue(Dimensions.version.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + version, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_phone_model) {
					String model = StringUtils.isBlank(log.getPhoneModel()) ? Constant.OTHER_MODEL : log.getPhoneModel();
					MapReduceHelpler.outputKeyValue(Dimensions.phone_model.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + model, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_phone_apn) {
					String apn = StringUtils.isBlank(log.getPhoneApn()) ? Constant.OTHER_APN : log.getPhoneApn();
					MapReduceHelpler.outputKeyValue(Dimensions.phone_apn.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + apn, indicatorType + keyValue, v_str, context);
					// 运营商-所有
					MapReduceHelpler.outputKeyValue(Dimensions.phone_apn.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + Constant.ALLAPP, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_phone_resolution) {
					String resolution = StringUtils.isBlank(log.getPhoneResolution()) ? Constant.OTHER_RESOLUTION : log.getPhoneResolution();
					MapReduceHelpler.outputKeyValue(Dimensions.phone_resolution.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + resolution, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_os) {
					String os = StringUtils.isBlank(log.getOs()) ? Constant.OTHER_OS : log.getOs();
					String osVer = StringUtils.isBlank(log.getPhoneFirmwareVersionNormal()) ? Constant.OTHER_OS : log.getPhoneFirmwareVersionNormal();
					// 操作系统下面再分版本
					MapReduceHelpler.outputKeyValue(Dimensions.os.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + os + Constant.DELIMITER + osVer, indicatorType + keyValue, v_str, context);
					// 操作系统-所有
					// MapReduceHelpler.outputKeyValue(Dimensions.os.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + Constant.ALLAPP + Constant.DELIMITER + osVer, indicatorType + keyValue, v_str,
					// context);
				}
				if (dim_type_cpid_code) {
					String cpidCode = StringUtils.isBlank(log.getCpid()) ? Constant.OTHER_RESOLUTION : log.getCpid();
					MapReduceHelpler.outputKeyValue(Dimensions.cpid_code.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + cpidCode, indicatorType + keyValue, v_str, context);
				}
				if (dim_type_all) {
					// 维度-所有
					MapReduceHelpler.outputKeyValue(Dimensions.all.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + Constant.ALLAPP, indicatorType + keyValue, v_str, context);
				}
			}
		}

		protected void outputKeyValueForApnAnalyse(AppLogFormat log, Context context, String indicatorType, String keyValue, String v_str) throws IOException, InterruptedException {
			String appkey = StringUtils.isBlank(log.getAppkey()) ? Constant.OTHER_APP : log.getAppkey();
			if (dim_type_os_and_cpid) {
				String os = StringUtils.isBlank(log.getOs()) ? Constant.OTHER_OS : log.getOs();
				// 二级渠道
				String cpid = log.getSalechannel2();
				MapReduceHelpler.outputKeyValue(Dimensions.osAndCpid.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + os + Constant.DELIMITER + cpid, indicatorType + keyValue, v_str, context);
				// 一级渠道
				cpid = log.getSalechannel1();
				MapReduceHelpler.outputKeyValue(Dimensions.osAndCpid.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + os + Constant.DELIMITER + cpid, indicatorType + keyValue, v_str, context);
				// 所有渠道
				cpid = log.getSalechannel1All();
				MapReduceHelpler.outputKeyValue(Dimensions.osAndCpid.toString() + Constant.DELIMITER + appkey + Constant.DELIMITER + os + Constant.DELIMITER + cpid, indicatorType + keyValue, v_str, context);
			}
		}

	}

	public static class ClientIndicatorReducer extends Reducer<TextKey, Text, ReducerCollectorClient, NullWritable> {
		private String	dateTime;
		private String	jobId;

		private boolean	number_users		= false;
		private boolean	number_new_users	= false;
		private boolean	number_active_users	= false;
		private boolean	times_start			= false;
		private boolean	time_average		= false;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.jobId = context.getJobID().toString();

			Configuration conf = context.getConfiguration();
			List<Indicators> indicators = MRDriver.loadIndicators(conf);// 加载解析指标
			this.number_users = indicators.contains(Indicators.NUMBER_USERS);
			this.number_new_users = indicators.contains(Indicators.NUMBER_NEW_USERS);
			this.number_active_users = indicators.contains(Indicators.NUMBER_ACITVE_USERS);
			this.times_start = indicators.contains(Indicators.TIMES_START);
			this.time_average = indicators.contains(Indicators.TIME_AVERAGE);
		}

		protected void reduce(TextKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 用户数
			long numberUsers = 0L;
			// 新增用户数
			long numberNewUsers = 0L;
			// 活跃用户数
			long numberActiveUsers = 0L;
			// 启动次数
			long timesStart = 0L;

			List<SurvivalTimeUnit> s = new ArrayList<SurvivalTimeUnit>();

			// 辅助计算活跃用户数的游标
			String tempStr = "";
			String curUDID = "";// 当前用户的UDID
			String newTag = ZERO; // 标识当前用户是否是新增用户
			long cursor = 0L; // 记录当前用户的启动次数

			String distinct = "";
			String[] keyArr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String indicatorType = key.getValue().toString().substring(0, 1);
			// 辅助计算平均使用时长
			SurvivalTimeUnit sur = null;
			long et = 0L;
			long surTime = 0L;

			for (Text v : values) {
				indicatorType = key.getValue().toString().substring(0, 1);
				if (!distinct.trim().equals(key.getValue().toString())) { // 去重
					if (indicatorType.equals(Constant.NUMBER_USERS)) {
						numberUsers += Long.valueOf(v.toString());
					}
					if (indicatorType.equals(Constant.NUMBER_NEW_USERS)) {
						numberNewUsers += Long.valueOf(v.toString());
					}
				}
				if (indicatorType.equals(Constant.NUMBER_ACITVE_USERS)) {
					tempStr = key.getValue().toString();

					if (!curUDID.trim().equals(tempStr.substring(0, tempStr.length() - 1))) {// 新用户到来
						if ((ONE.equals(newTag) && cursor > 1) || (ZERO.equals(newTag) && cursor > 0)) {
							numberActiveUsers++;
						}
						newTag = tempStr.substring(tempStr.length() - 1);
						cursor = 0;
					} else {// 老用户到来
						if (ONE.equals(tempStr.substring(tempStr.length() - 1))) {
							newTag = ONE;
						}
						cursor++;
					}
					curUDID = tempStr.substring(0, tempStr.length() - 1);
				}
				if (indicatorType.equals(Constant.TIMES_START)) {
					timesStart += Long.valueOf(v.toString());
				}
				if (indicatorType.equals(Constant.TIME_AVERAGE)) {
					// 时间是降序来到
					sur = new SurvivalTimeUnit(Long.valueOf(key.getValue().toString().substring(1, key.getValue().toString().length() - 1)), key.getValue().toString().substring(key.getValue().toString().length() - 1));
					// 时间为负的情况，直接丢弃不用
					if (sur.getTime() > 0) {
						// s.add(sur);
						if (et > 0) {
							surTime += (et - sur.getTime());
						}
						if (ONE.equals(sur.getTag())) {
							et = 0;
						}
						if (ZERO.equals(sur.getTag())) {
							et = sur.getTime();
						}

					}
				}
				distinct = key.getValue().toString();
			}

			String extension_code = "-";
			if (keyArr.length > 3) {
				extension_code = keyArr[3];
			}
			long survivalTime = 0L;
			// Collections.sort(s, new SurvivalTimeComparator());
			survivalTime = surTime - SurvivalTimeUtil.getSurvivalTime(s);

			if (survivalTime < 0) {
				survivalTime = 0;
			}
			// 开始写库
			ReducerCollectorClient o = null;
			if (this.number_users) {

				o = new ReducerCollectorClient(jobId, dateTime, keyArr[0], keyArr[1], keyArr[2], extension_code, "-", Constant.NUMBER_USERS, Float.valueOf(numberUsers + ""));

				context.write(o, NullWritable.get());
			}
			if (this.number_new_users) {

				o = new ReducerCollectorClient(jobId, dateTime, keyArr[0], keyArr[1], keyArr[2], extension_code, "-", Constant.NUMBER_NEW_USERS, Float.valueOf(numberNewUsers + ""));

				context.write(o, NullWritable.get());
			}
			if (this.number_active_users) {

				// 计算活跃用户数时，由过去决定当前，最后一趟没跑，所以此处看情况是否要+1;
				if ((ONE.equals(newTag) && cursor > 1) || (ZERO.equals(newTag) && cursor > 0)) {
					numberActiveUsers++;
				}

				o = new ReducerCollectorClient(jobId, dateTime, keyArr[0], keyArr[1], keyArr[2], extension_code, "-", Constant.NUMBER_ACITVE_USERS, Float.valueOf(numberActiveUsers + ""));

				context.write(o, NullWritable.get());
			}
			if (this.times_start) {

				o = new ReducerCollectorClient(jobId, dateTime, keyArr[0], keyArr[1], keyArr[2], extension_code, "-", Constant.TIMES_START, Float.valueOf(timesStart + ""));

				context.write(o, NullWritable.get());
			}
			if (this.time_average) {

				float avg = 0;
				if (numberUsers > 0) {
					avg = survivalTime / numberUsers;
				}

				o = new ReducerCollectorClient(jobId, dateTime, keyArr[0], keyArr[1], keyArr[2], extension_code, "-", Constant.TIME_AVERAGE, avg);

				context.write(o, NullWritable.get());
			}
		}
	}

}
