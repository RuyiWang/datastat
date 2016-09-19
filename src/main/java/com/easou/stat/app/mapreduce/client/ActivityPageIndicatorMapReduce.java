package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

import com.easou.stat.app.constant.ActivityDimensions;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorActivityPage;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityPageIndicatorMapReduce.java
 * @Description: 页面分析MR
 * @author: Asa
 * @date: 2014年5月13日 下午4:31:01
 *
 */
public class ActivityPageIndicatorMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityPageIndicatorMapReduce.class);

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
		DBOutputFormat.setOutput(job, ReducerCollectorActivityPage.getTableNameByGranularity(), ReducerCollectorActivityPage.getFields());

		job.setMapperClass(ActivityPageIndicatorMapper.class);
		job.setReducerClass(ActivityPageIndicatorReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setOutputKeyClass(ReducerCollectorActivityPage.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class ActivityPageIndicatorMapper extends Mapper<LongWritable, Text, TextKey, Text> {
		
		private long time = 0L;
		private String granularity = null;

		@Override
		protected void map(LongWritable l, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			String dateFormat = "yyyyMMddHHmmss";
			Date date = null;
			Map<String, Object> lineMap = Json.fromJson(Map.class, _line.toString());
			String appkey = (String) lineMap.get("appkey");
			appkey = StringUtils.trimToNull(appkey);
			String version = (String) lineMap.get("phone_softversion");
			version = StringUtils.trimToNull(version);
			if(appkey == null || version == null)
				return;
			String peroid = "0";
			if (Granularity.HOUR.toString().equals(this.granularity)) {
				date = DateUtil.getFirstMinuteOfHour(this.time);
			} else if (Granularity.DAY.toString().equals(this.granularity)) {
				date = DateUtil.getFirstHourOfDay(this.time);
				peroid = "1";
			} else if (Granularity.WEEK.toString().equals(this.granularity)) {
				date = DateUtil.getMondayOfWeek(this.time);
				peroid = "7";
			} else {
				date = DateUtil.getFirstDayOfMonth(this.time);
				peroid = "30";
			}
			String dateTime = CommonUtils.getDateTime(date.getTime(), dateFormat);
			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, version, true);
			dims = composeDims(dims, dateTime, false);
			dims = composeDims(dims, peroid, false);
			dims = composeDims(dims, "pagename", false);
			List<Map<String, Object>> activities = (List<Map<String, Object>>) lineMap.get("activities");
			for(Map<String, Object> activity : activities) {
				write(dims, activity, context);
			}
		}
		/**
		 * simple eventInfo 
		 * @param dims
		 * @param lineMap
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void write(List<String> dims, Map<String, Object> activity, Context context) throws IOException, InterruptedException {
			String curActivity = (String) activity.get("current_activity");
			String useTime = (String) activity.get("activity_use_time");
			String referActivity = (String) activity.get("refer_activity");
			for(String dim : dims) {
				String timesKey = dim + Constant.DELIMITER + Constant.COUNT;
				String timeKey = dim + Constant.DELIMITER + Constant.AVG;
				String referKey = dim + Constant.DELIMITER + Constant.COUNT;
				if(StringUtils.isNotBlank(curActivity) && !"wifi".equals(curActivity)){
					write(context, timesKey + Constant.DELIMITER + curActivity + Constant.DELIMITER + ActivityDimensions.acttimes.toString(), "1");
					write(context, timesKey + Constant.DELIMITER + "all" + Constant.DELIMITER + ActivityDimensions.acttimes.toString(), "1");
				}
				
				if(StringUtils.isNotEmpty(useTime) && StringUtils.isNotBlank(curActivity)) {
					write(context, timeKey + Constant.DELIMITER + curActivity + Constant.DELIMITER + ActivityDimensions.acttime.toString(), useTime);
					write(context, timeKey + Constant.DELIMITER + "all" + Constant.DELIMITER + ActivityDimensions.acttime.toString(), useTime);
				}
				
				if(StringUtils.isNotEmpty(referActivity) && !"wifi".equals(referActivity)) {
					write(context, referKey + Constant.DELIMITER + referActivity + Constant.DELIMITER + ActivityDimensions.reftimes.toString(), "1");
					write(context, referKey + Constant.DELIMITER + "all" + Constant.DELIMITER + ActivityDimensions.reftimes.toString(), "1");
				}
			}
		}
		
		private List<String> composeDims(List<String> prefixs, String dim, boolean isAll) {
			List<String> list = new ArrayList<String>();
			if(prefixs == null || prefixs.size() == 0) {
				list.add(dim);
				if(isAll)
					list.add("all");
			} else {
				for(String prefix : prefixs) {
					list.add(prefix + Constant.DELIMITER + dim);
					if(isAll)
						list.add(prefix + Constant.DELIMITER + "all");
				}
			}
			return list;
		}
		private List<String> composeDims(List<String> prefixs, String dim, String dimCode, boolean isAll) {
			List<String> list = new ArrayList<String>();
			dimCode = StringUtils.trimToNull(dimCode);
			if(prefixs == null || prefixs.size() == 0) {
				list.add(dim + Constant.DELIMITER + dimCode);
				if(isAll)
					list.add("all");
			} else {
				for(String prefix : prefixs) {
					if(StringUtils.isNotEmpty(dimCode)) 
						list.add(prefix + Constant.DELIMITER + dim + Constant.DELIMITER + dimCode);
					if(isAll) {
						list.add(prefix + Constant.DELIMITER + dim + Constant.DELIMITER + "all");
						//list.add(prefix + Constant.DELIMITER + "all" + Constant.DELIMITER + "all");
					}
				}
			}
			return list;
		}
		
		private void write(Context context, String key, String value) throws IOException, InterruptedException {
			Text textValue = new Text(value);
			TextKey textKey = new TextKey(new Text(key), textValue);
			context.write(textKey, textValue);
		}
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			this.granularity = config.get(Constant.GRANULARITY);
			String dateTime = config.get(Constant._DATETIME);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			try {
				this.time = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				LOG_MR.error(e);
			}
		}
	}

	public static class ActivityPageIndicatorReducer extends Reducer<TextKey, Text, ReducerCollectorActivityPage, NullWritable> {
		private String jobId = null;
		protected void reduce(TextKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println(key.getDimension().toString());
			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String version = keyStr[1];
			String dateTime = keyStr[2];
			String peroid = keyStr[3];
			String dim = keyStr[4];
			String method = keyStr[5];
			String dimCode = keyStr[6];
			String indicator = keyStr[7];
			
			float result = 0.0f;
			if(Constant.AVG.equalsIgnoreCase(method)) {
				int count = 0;
				long sum = 0l;
				for(Text value : values) {
					count ++;
					sum += Long.parseLong(value.toString());
				}
				result = sum / count;
			} else if(Constant.SUM.equalsIgnoreCase(method)){ 
				for(Text value : values) {
					result += Float.parseFloat(value.toString());
				}
			} else {
				int count = 0;
				for(Text value : values) {
					count ++;
				}
				result = count;
			}
			ReducerCollectorActivityPage outData = new ReducerCollectorActivityPage();
			outData.setAppkey(appKey);
			outData.setDim_type(dim);
			outData.setDim_code(dimCode);
			outData.setIndicator(indicator);
			outData.setJobid(this.jobId);
			outData.setPhone_softversion(version);
			outData.setStat_date(dateTime);
			outData.setTime_peroid(peroid);
			outData.setValue(result);
			context.write(outData, NullWritable.get());
		}

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
	
	
	public static void main(String[] args) throws UnsupportedEncodingException {
//		String str = "{\"appkey\":\"3a0640d4013a0640d44e0000\",\"cpid\":\"jlxjo\",\"phone_softversion\":\"0.851\",\"type\":\"type_eventinfo\",\"sdk_version\":\"1.7\",\"phone_udid\":\"e677f86101bd6bc21a184576976dad57749f5cd9\",\"currentnetworktype\":\"wifi\",\"gatewayip\":\"222.161.184.127\",\"phone_apn\":\"中国移动\",\"phone_city\":\"北京\",\"events\":[{\"event_id\":\"id_start\",\"time\":\"1359112474890\",\"status\":\"0\",\"event_paralist\":{}},{\"event_id\":\"id_webview\",\"time\":\"1359112474890\",\"status\":\"0\",\"event_paralist\":{\"url\":\"http: //www.sina.com.cn/topic/ab.html\"}},{\"event_id\":\"id_push_token\",\"time\":\"1359112474894\",\"status\":\"0\",\"event_paralist\":{\"token\":\"e05a44a899b121bd21eccf41791f7b945cda66a3c60598de53f85414 b69efc8f\"}},{\"event_id\":\"id_push_click\",\"time\":\"1359112474900\",\"status\":\"0\",\"event_paralist\":{\"fileid\":\"14\"}},{\"event_id\":\"id_search_subs\",\"time\":\"1359112474912\",\"status\":\"0\",\"event_paralist\":{\"type\":\"新闻\",\"searchword\":\"范冰冰\",\"item\":\"368_网易科技\"}},{\"event_id\":\"id_websearch_click\",\"time\":\"1359112474915\",\"status\":\"0\",\"event_paralist\":{\"clickid\":\"123\",\"cpid\":\"bjp100_10959_001\",\"searchword\":\"范冰冰\"}},{\"event_id\":\"id_share\",\"time\":\"1359112474915\",\"status\":\"0\",\"event_paralist\":{\"share_type\":\"新闻\",\"share_id\":\"Qzone\"}},{\"event_id\":\"id_pic_save\",\"time\":\"1359112474920\",\"status\":\"0\",\"event_paralist\":{\"saveto\":\"相册\"}},{\"event_id\":\"id_night_mode\",\"time\":\"1359112474922\",\"status\":\"0\",\"event_paralist\":{\"on-off\":\"on\"}},{\"event_id\":\"id_apply_click\",\"time\":\"1359112474928\",\"status\":\"0\",\"event_paralist\":{\"item\":\"55_91手机助手\",\"cpid\":\"bjp100_10959_001\"}}]}";
//		Map obj = Json.fromJson(Map.class, str);
//		List<Map> arr = (List) obj.get("events");
//		for(Map o : arr) {
//			System.out.println(o.get("event_id"));
//		}
//		System.out.println(arr.size());
		String url = "http: //www.sina.com.cn/topic/ab.html";
		
		System.out.println(url.length() + "," + url.getBytes("utf-8").length);
	}
}
