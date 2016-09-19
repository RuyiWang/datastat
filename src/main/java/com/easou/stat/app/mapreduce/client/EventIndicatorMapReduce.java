package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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
import com.easou.stat.app.constant.EventDimensions;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorEvent;
import com.easou.stat.app.schedule.db.EventDefineModel;
import com.easou.stat.app.schedule.db.EventDefineModel.DimDefineModel;
import com.easou.stat.app.schedule.db.EventDefineModel.StatDefineModel;
import com.easou.stat.app.schedule.util.EventRuleUtil;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: EventIndicatorMapReduce.java
 * @Description:事件分析MR 
 * @author: Asa
 * @date: 2014年6月5日 上午11:10:20
 *
 */
public class EventIndicatorMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(EventIndicatorMapReduce.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "false"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "false"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		
		DistributedCache.addCacheFile(new URI(Constant.EVENT_CACHE), getConf());
		DistributedCache.createSymlink(getConf());
		
		MRDriver op = new MRDriver(getConf(), Json.toJson(map), false);
		
		Job job = op.getJob();
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorEvent.getTableNameByGranularity(op.getGranularity()), ReducerCollectorEvent.getFields());

		job.setMapperClass(EventIndicatorMapper.class);
		job.setReducerClass(EventIndicatorReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setOutputKeyClass(ReducerCollectorEvent.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class EventIndicatorMapper extends Mapper<LongWritable, Text, TextKey, Text> {
		private EventRuleUtil eventRuleUtil = null;
		private Path[] localFiles;
		private long time = 0L;
		private String granularity = null;
		
		@SuppressWarnings("unchecked")
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
			List<Map<String, Object>> events = (List<Map<String, Object>>) lineMap.get("events");
			String udid = (String) lineMap.get("phone_udid");
			for(int i = 0, size = events.size(); i < size; i ++) {
				Object obj = events.get(i);
				if(obj instanceof Map<?, ?>) {
					Map<String, Object> event = (Map<String, Object>) obj;
					String eventId = (String) event.get("event_id");
					EventDefineModel model = eventRuleUtil.getEvent(eventId+appkey);
					if(model == null) {
						continue;
					}
					List<String> eventDims = composeDims(dims, eventId, false);
					if("1".equals(model.getType())) {
						writeTypeOne(eventDims, udid, context);
					} else {
						Map<String, String> params = (Map<String, String>) event.get("event_paralist");
						if(params == null || params.isEmpty())
							continue;
						writeTypeTwo(model, eventDims, udid, params, context);
					}
				}
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
		private void writeTypeOne(List<String> dims, String udid, Context context) throws IOException, InterruptedException {
			for(String dim : dims) {
				String key = dim + Constant.DELIMITER + "all" + Constant.DELIMITER + "all";
				write(context, key + Constant.DELIMITER + Constant.DISTINCT + Constant.DELIMITER + EventDimensions.evtudids.toString(), udid);
				write(context, key + Constant.DELIMITER + Constant.COUNT + Constant.DELIMITER + EventDimensions.evttimes, "1");
			}
		}
		/**
		 * complex eventInfo
		 * @param model
		 * @param dims
		 * @param lineMap
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void writeTypeTwo(EventDefineModel model, List<String> dims, String udid, Map<String, String> params, Context context) throws IOException, InterruptedException {
			List<String> allList = composeDims(dims, "all", "all", false);
			for(String all : allList) {
				write(context, all + Constant.DELIMITER + Constant.DISTINCT + Constant.DELIMITER + EventDimensions.evtudids.toString(), udid);
				write(context, all + Constant.DELIMITER + Constant.COUNT + Constant.DELIMITER + EventDimensions.evttimes, "1");
			}
			List<DimDefineModel> dimModels = model.getDims();
			for(DimDefineModel dimModel : dimModels) {
				String dimType = dimModel.getId();
				String dimCode = "all";
				if(StringUtils.isEmpty(dimType)) {
					dimType = "all";
				} else {
					dimCode = getDimCode(dimType, params);
				}
				dimCode = StringUtils.trimToNull(dimCode);
				if(dimCode != null) {
					List<String> dimList = composeDims(dims, dimType, dimCode, dimModel.isAll());
					List<StatDefineModel> statModels = dimModel.getStats();
					for(String dimStr : dimList) {
						write(context, dimStr + Constant.DELIMITER + Constant.DISTINCT + Constant.DELIMITER + EventDimensions.evtudids.toString(), udid);
						write(context, dimStr + Constant.DELIMITER + Constant.COUNT + Constant.DELIMITER + EventDimensions.evttimes, "1");
						if(!statModels.isEmpty()) {
							for(StatDefineModel statModel : statModels) {
								String indicator = statModel.getId();
								String value = params.get(indicator);
//								if("cpid".equals(indicator)) {
//									value = SaleChannelUtil.getInstance().getSaleChannelByCpid(value, (String) lineMap.get("os"));
//								}
								if(Constant.DISTINCT.equalsIgnoreCase(statModel.getMethod())) {
									write(context, dimStr + Constant.DELIMITER + Constant.DISTINCT + Constant.DELIMITER + indicator, value);
								} else if(Constant.SUM.equalsIgnoreCase(statModel.getMethod())) {
									write(context, dimStr + Constant.DELIMITER + Constant.SUM + Constant.DELIMITER + indicator, value);
								} else {
									write(context, dimStr + Constant.DELIMITER + Constant.COUNT + Constant.DELIMITER + indicator, value);
								}
							}
						}
						
					}
				}
			}
		}
		
		private String getDimCode(String dimType, Map<String, String> params) {
			String[] strs = dimType.split(",");
			StringBuffer sb = new StringBuffer();
			for(int i = 0, length = strs.length; i < length; i ++) {
				if(i > 0)
					sb.append(",");
				String value = params.get(strs[i]);
				if(StringUtils.isEmpty(value))
					return null;
				if("clickid".equalsIgnoreCase(strs[i])) {
					int num = Integer.parseInt(value);
					if(num > 10)
						value = "10+";
				} else if("url".equalsIgnoreCase(strs[i])) {
					value = getDomain(value);
				}
				sb.append(value);
			}
			if(sb.length() > 200)
				return sb.substring(0, 200);
			return sb.toString();
		}
		
		private String getDomain(String url) {
			int index = url.indexOf("//");
			if(index > -1)
				url = url.substring(index + 2);
			int end = url.indexOf("/");
			if(end > 0)
				url = url.substring(0, end);
			return url;
		}
		private String getFirstChannel(String saleChannel) {
			if(!saleChannel.endsWith("00000000")) {
				return saleChannel.substring(0, 4) + "00000000";
			}
			return null;
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
			eventRuleUtil = EventRuleUtil.getInstance();
			localFiles = DistributedCache.getLocalCacheFiles(config);;
			eventRuleUtil.importRules(localFiles);
			try {
				this.time = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				LOG_MR.error("时间转换异常",e);
			}
			
		}
	}

	public static class EventIndicatorReducer extends Reducer<TextKey, Text, ReducerCollectorEvent, NullWritable> {
		private String jobId = null;
		
		protected void reduce(TextKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println(key.getDimension().toString());
			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String version = keyStr[1];
			String dateTime = keyStr[2];
			String peroid = keyStr[3];
			String eventId = keyStr[4];
			String dim = keyStr[5];
			String dimCode = keyStr[6];
			String method = keyStr[7];
			String indicator = keyStr[8];
			float result = 0.0f;
			if(Constant.DISTINCT.equalsIgnoreCase(method)) {
				int count = 0;
				String word = "";
				for(Text value : values) {
					if(!word.equals(value.toString())) {
						count ++;
						word = value.toString();
					}
				}
				result = count;
			} else if(Constant.SUM.equalsIgnoreCase(method)){ 
				for(Text value : values) {
					try{
						result += Float.parseFloat(value.toString());
					}catch(Exception e){}
				}
			} else {
				int count = 0;
				for(Text value : values) {
					count ++;
				}
				result = count;
			}
			ReducerCollectorEvent outData = new ReducerCollectorEvent();
			outData.setAppkey(appKey);
			outData.setDim_type(dim);
			outData.setDim_code(dimCode);
			outData.setIndicator(indicator);
			outData.setJobid(this.jobId);
			outData.setPhone_softversion(version);
			outData.setStat_date(dateTime);
			outData.setTime_peroid(peroid);
			outData.setEvent_id(eventId);
			outData.setValue(result);
			context.write(outData, NullWritable.get());
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
	
}
