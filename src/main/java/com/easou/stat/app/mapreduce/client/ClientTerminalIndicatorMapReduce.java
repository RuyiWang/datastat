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

import com.easou.stat.app.constant.ActivityDimensions;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorTerminal;
import com.easou.stat.app.util.ApnUtil;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
import com.easou.stat.app.util.MessyCodeUtil;
import com.easou.stat.app.util.NetWorkTypeUtil;
/**
 * 
 * @ClassName: ClientTerminalIndicatorMapReduce.java
 * @Description: 终端分析MR step-2
 * @author: Asa
 * @date: 2014年5月13日 下午4:29:54
 *
 */
public class ClientTerminalIndicatorMapReduce extends Configured implements Tool{
	public static final Log LOG_MR = LogFactory.getLog(ClientTerminalIndicatorMapReduce.class);

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
		DBOutputFormat.setOutput(job, ReducerCollectorTerminal.getTableNameByGranularity(), ReducerCollectorTerminal.getFields());

		job.setMapperClass(ClientTerminalIndicatorMapper.class);
		job.setReducerClass(ClientTerminalIndicatorReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setOutputKeyClass(ReducerCollectorTerminal.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class ClientTerminalIndicatorMapper extends Mapper<LongWritable, Text, TextKey, Text> {
		
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
			String phoneCity = (String) lineMap.get("phone_city");
			String phoneFirmwareVersion = (String) lineMap.get("phone_firmware_version");
			String phoneModel = (String) lineMap.get("phone_model");
			String phoneResolution = (String) lineMap.get("phone_resolution");
			String cpid = (String) lineMap.get("cpid");
			String phoneApn = (String) lineMap.get("phone_apn");
			String currentnetworktype = (String) lineMap.get("currentnetworktype");
			
			phoneCity = getFormatCity(phoneCity);
			phoneApn = ApnUtil.getNetWorkType(phoneApn);
			currentnetworktype = NetWorkTypeUtil.getNetWorkType(currentnetworktype);
		
			//检查日志
			if(StringUtils.isBlank(appkey) || StringUtils.isBlank(version) || StringUtils.isBlank(cpid))
				return;
			
			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, version, true);
			dims = composeDims(dims, dateTime, false);
			dims = composeDims(dims, peroid, false);
			dims = composeDims(dims, cpid, true);
			
			List<String>  dimsPhoneCity = composeDims(dims, "phoneCity", phoneCity, true);
			List<String>  dimsPhoneFirmwareVersion = composeDims(dims, "phoneFirmwareVersion", phoneFirmwareVersion, true);
			List<String>  dimsPhoneResolution = composeDims(dims, "phoneResolution",phoneResolution, true);
			List<String>  dimsPhoneApn = composeDims(dims, "phoneApn", phoneApn, true);
			List<String>  dimCurrentnetworktype = composeDims(dims, "currentnetworktype", currentnetworktype, true);
			List<String>  dimPhoneModel = composeDims(dims, "phoneModel", phoneModel, true);

			write(dimsPhoneCity, lineMap, context);
			write(dimsPhoneFirmwareVersion, lineMap, context);
			write(dimsPhoneResolution, lineMap, context);
			write(dimsPhoneApn, lineMap, context);
			write(dimCurrentnetworktype, lineMap, context);
			write(dimPhoneModel, lineMap, context);
		}
		/**
		 * @Titile: getFormatCity
		 * @Description: 匹配地区
		 * @author: 
		 * @date: 2013年8月30日 上午10:45:43
		 * @return String
		 */
		private String getFormatCity(String phoneCity) {
			if(StringUtils.isEmpty(phoneCity))
				return "其他";
			phoneCity = MessyCodeUtil.changeStrCode(phoneCity);
			//去重诸如"湖北省湖北省黄冈市"类型的省份
			phoneCity = delDuplicate(phoneCity, "省");
			phoneCity = delDuplicate(phoneCity, "自治区");
			//保留到市
			int index = phoneCity.indexOf("市");
			if(index > 0)
				return phoneCity.substring(0, index + 1);
			//保留到自治区自治州
			index = phoneCity.indexOf("自治");
			if(index > 0)
				return phoneCity.substring(0, index + 3);
			//保留到省份地区
			index = phoneCity.indexOf("地区");
			if(index > 0)
				return phoneCity.substring(0, index + 2);
			index = phoneCity.indexOf("特别行政区");
			if(index > 0)
				return phoneCity.substring(0, index + 5);
			//只留省份
			index = phoneCity.indexOf("省");
			if(index > 0)
				return phoneCity.substring(0, index + 1);
			return phoneCity;
		}
		private String delDuplicate(String str, String sub) {
			int subLength = sub.length();
			int index = str.indexOf(sub);
			while(index > 0) {
				int tmp = str.indexOf(sub, index + subLength);
				if(tmp > index){
					str = str.substring(index + subLength);
					index = tmp - index - 1;
				} else {
					break;
				}
			}
			return str;
		}
		
		/**
		 * @param dims
		 * @param lineMap
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void write(List<String> dims, Map<String, Object> lineMap, Context context) throws IOException, InterruptedException {

			String udid = (String) lineMap.get("phone_udid");
			String phone_esid = (String) lineMap.get("phone_esid");
			
			for(String dim : dims) {
				write(context, dim + Constant.DELIMITER + ActivityDimensions.actusers.toString(), udid, udid);
				write(context, dim + Constant.DELIMITER + ActivityDimensions.starttimes.toString(), phone_esid, phone_esid);
			}
		}
		/**
		 * @Titile: composeDims
		 * @Description: 组合统计维度
		 * @author: 
		 * @date: 2013年8月30日 上午10:46:14
		 * @return List<String>
		 */
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
		
		/**
		 * @Titile: composeDims
		 * @Description: 
		 * @author: Asa
		 * @date: 2013年8月30日 上午10:52:02
		 * @return List<String>
		 */
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
					if(isAll)
						list.add(prefix + Constant.DELIMITER + dim + Constant.DELIMITER + "all");
				}
			}
			return list;
		}
		
		private void write(Context context, String key, String keyValue, String value) throws IOException, InterruptedException {

			TextKey textKey = new TextKey(new Text(key), new Text(keyValue));
			context.write(textKey, new Text(value));
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
				LOG_MR.error("时间转换异常",e);
			}
		}
	}

	public static class ClientTerminalIndicatorReducer extends Reducer<TextKey, Text, ReducerCollectorTerminal, NullWritable> {
		private String jobId = null;

		protected void reduce(TextKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String version = keyStr[1];
			String dateTime = keyStr[2];
			String peroid = keyStr[3];
			String cpid = keyStr[4];
			String dim_type = keyStr[5];
			String dim_code = keyStr[6];
			String indicator = keyStr[7];
			
			
			ReducerCollectorTerminal outData = new ReducerCollectorTerminal();
			outData.setJobid(jobId);
			outData.setAppkey(appKey);
			outData.setPhone_softversion(version);
			outData.setStat_date(dateTime);
			outData.setTime_peroid(peroid);
			outData.setCpid(cpid);
			outData.setDim_type(dim_type);
			outData.setDim_code(dim_code);
			outData.setIndicator(indicator);
			
			if(ActivityDimensions.actusers.toString().equalsIgnoreCase(indicator) || ActivityDimensions.starttimes.toString().equalsIgnoreCase(indicator)) {
				int count = 0;
				String word = "";
				for(Text value : values) {
					if(!word.equals(value.toString())) {
						count ++;
						word = value.toString();
					}
				}
				outData.setIndicator(indicator);
				outData.setValue((float)count);
				context.write(outData, NullWritable.get());
			} 
//			else if("compose".equalsIgnoreCase(indicator)){
//				String phone_esid_pre = "";
//				// 平均单次使用时长
//				long useTime = 0L;
//				long lastTime = 0L;
//				// 平均上行流量
//				long totalUp = 0L;
//				long lastUp = 0L;
//				// 平均下行流量
//				long totalDown = 0L;
//				long lastDown = 0L;
//				int count = 0;
//				for (Text value : values) {
//					Map<String, Object> lineMap = Json.fromJson(Map.class,value.toString());
//					String phone_esid = (String) lineMap.get("phone_esid");
//					if (!phone_esid_pre.equals(phone_esid)) {
//						phone_esid_pre = phone_esid;
//						useTime -= lastTime;
//						lastTime = 0L;
//
//						totalUp -= lastUp;
//						lastUp = 0L;
//
//						totalDown -= lastDown;
//						lastDown = 0L;
//
//						count++;
//					}
//					List<Map<String, Object>> activities = (List<Map<String, Object>>) lineMap.get("activities");
//					for (Map<String, Object> act : activities) {
//						//使用时长
//						String use_timeName = act.get("activity_use_time").getClass().getName();
//						long actTime = 0L;
//						if ("java.lang.String".equals(use_timeName)) {
//							if (StringUtils.isNotEmpty((String) act.get("activity_use_time"))) {
//								actTime = Long.parseLong((String) act.get("activity_use_time"));
//							}
//						} else if ("java.lang.Integer".equals(use_timeName)) {
//							actTime = (Integer) act.get("activity_use_time");
//						} else {
//							actTime = (Long) act.get("activity_use_time");
//						}
//						useTime += actTime;
//						lastTime = actTime;
//						
//						//上行流量
//						String up_trafficName = act.get("up_traffic").getClass().getName();
//						long up_traffic = 0L;
//						if("java.lang.String".equals(up_trafficName)){
//							if (StringUtils.isNotEmpty((String) act.get("up_traffic"))) {
//								up_traffic = Long.parseLong((String) act.get("up_traffic"));
//							}
//						}else if ("java.lang.Integer".equals(up_trafficName)){
//							up_traffic = (Integer) act.get("up_traffic");
//						} else {
//							up_traffic = (Long) act.get("up_traffic");
//						}
//					
//						totalUp += up_traffic;
//						lastUp = up_traffic;
//					
//						//下行流量
//						String down_trafficName = act.get("down_traffic").getClass().getName();
//						long down_traffic = 0L;
//						if("java.lang.String".equals(down_trafficName)){
//							if (StringUtils.isNotEmpty((String) act.get("down_traffic"))) {
//								down_traffic = Long.parseLong((String) act.get("down_traffic"));
//							}
//						}else if ("java.lang.Integer".equals(down_trafficName)){
//							down_traffic = (Integer) act.get("down_traffic");
//						} else {
//							down_traffic = (Long) act.get("down_traffic");
//						}
//						
//						totalDown += down_traffic;
//						lastDown = down_traffic;
//				
//					}
//
//				}
//
//				// 平均单次使用时长
//				
//				outData.setIndicator("pt_usetime");
//				outData.setValue((float) useTime / (count*1000));
//				context.write(outData, NullWritable.get());
//				// 平均上行流量
//				
//				outData.setIndicator("pp_up_traffic");
//				outData.setValue((float) totalUp / (count*1024));
//				context.write(outData, NullWritable.get());
//				// 平均下行流量
//				
//				outData.setIndicator("pp_down_traffic");
//				outData.setValue((float) totalDown / (count*1024));
//				context.write(outData, NullWritable.get());
//			}
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
	
}
