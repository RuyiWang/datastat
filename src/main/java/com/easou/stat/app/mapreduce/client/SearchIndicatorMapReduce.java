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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.constant.SearchDimensions;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorSearch;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: SearchIndicatorMapReduce.java
 * @Description: 搜索分析-基础指标
 * @author: Asa
 * @date: 2014年6月5日 上午11:09:57
 *
 */
public class SearchIndicatorMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(SearchIndicatorMapReduce.class);

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
		DBOutputFormat.setOutput(job, ReducerCollectorSearch.getTableNameByGranularity(op.getGranularity()), ReducerCollectorSearch.getFields());

		job.setMapperClass(SearchIndicatorMapper.class);
		job.setReducerClass(SearchIndicatorReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setOutputKeyClass(ReducerCollectorSearch.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class SearchIndicatorMapper extends Mapper<LongWritable, Text, TextKey, Text> {
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
			JSONObject lineMap = JSON.parseObject(_line.toString());
			Integer pageId =  lineMap.getInteger("page_id");
			if(pageId == null) {
				return;
			}
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
			String appkey =  lineMap.getString("appkey");
			if(StringUtils.isBlank(appkey))
				return;
			String version =  lineMap.getString("phone_softversion");
			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, version, true);
			dims = composeDims(dims, dateTime, false);
			dims = composeDims(dims, peroid, false);
			//channel
			String cpid = lineMap.getString("cpid");
			List<String> cpidDims = composeDims(dims, "cpid", cpid, true);
			
			writeByDim(cpidDims, lineMap, context);
			//page
			String searchPage = (String) lineMap.getString("searchpage");
			List<String> pageDims = composeDims(dims, "searchpage", searchPage, true);
			writeByDim(pageDims, lineMap, context);
			//searchSource
			/**
			 * 2014年2月24日 新版搜索日志没有此字段
			Map<String, String> searchwordSource = (Map<String, String>) lineMap.get("searchword_source");
			String sourceType = searchwordSource.get("source_type");
			List<String> typeDims = composeDims(dims, "sourcetype", sourceType, true);
			writeByDim(typeDims, lineMap, context);
			 */
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
					if(isAll)
						list.add(prefix + Constant.DELIMITER + dim + Constant.DELIMITER + "all");
				}
			}
			return list;
		}
		private void writeByDim(List<String> dims, JSONObject lineMap, Context context) throws IOException, InterruptedException {
			Integer pageId =  lineMap.getInteger("page_id");
			for(String dim : dims) {
				if(pageId == 1) {
					//
					String key = dim + Constant.DELIMITER + SearchDimensions.stimes.toString();
					String value = "1";
					write(context, key, value);
				}
				if(pageId == 1) {
					int ss =  lineMap.getInteger("searchresults");
					boolean isNoResult = true;
					
					if(ss > 0) {
						isNoResult = false;
					}
				
					if(isNoResult) {
						String key = dim + Constant.DELIMITER + SearchDimensions.s0times.toString();
						String value = "1";
						write(context, key, value);
					}
				}
				if(pageId == 1) {
					String searchaction =  lineMap.getString("searchaction");
					String key = dim + Constant.DELIMITER;
					if("1".equals(searchaction)) {
						key += SearchDimensions.sact1times.toString();
					} else {
						key += SearchDimensions.sact0times.toString();
					}
					String value = "1";
					write(context, key, value);
				}
				if(pageId == 1) {
					String value =  lineMap.getString("searchword");
					value = StringUtils.trimToNull(value);
					if(value != null) {
						String key = dim + Constant.DELIMITER + SearchDimensions.swords.toString();
						write(context, key, value.toLowerCase());
					}
				}
//				if(pageId == 1) {
//					Map<String, String> searchwordSource = (Map<String, String>) lineMap.get("searchword_source");
//					String word = searchwordSource.get("refer_searchword");
//					word = StringUtils.trimToNull(word);
//					if(word != null) {
//						String key = dim + Constant.DELIMITER + SearchDimensions.sswords.toString();
//						write(context, key, word.toLowerCase());
//					}
//				}
				if(pageId == 1) {
					String key = dim + Constant.DELIMITER + SearchDimensions.sudids.toString();
					String value =  lineMap.getString("phone_udid");
					write(context, key, value);
				}
				write(context, dim + Constant.DELIMITER + SearchDimensions.sptimes.toString(), "1");
			}
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
				
				e.printStackTrace();
			}
		}
	}

	public static class SearchIndicatorReducer extends Reducer<TextKey, Text, ReducerCollectorSearch, NullWritable> {
		private String jobId = null;
		protected void reduce(TextKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String version = keyStr[1];
			String dateTime = keyStr[2];
			String peroid = keyStr[3];
			String dim = keyStr[4];
			String dimCode = keyStr[5];
			String indicator = keyStr[6];
//			if(!"all".equals(dim)) {
//				dimCode = keyStr[5];
//				indicator = keyStr[6];
//			} else {
//				indicator = keyStr[5];
//			}
			float result = 0.0f;
			if(SearchDimensions.swords.toString().equals(indicator) || SearchDimensions.sswords.toString().equals(indicator) || SearchDimensions.sudids.toString().equals(indicator)) {
				int count = 0;
				String word = "";
				for(Text value : values) {
					if(!word.equals(value.toString())) {
						count ++;
						word = value.toString();
					}
				}
				result = count;
			} else {
				int count = 0;
				for(Text value : values) {
					count += Integer.parseInt(value.toString());
				}
				result = count;
			}
			ReducerCollectorSearch outData = new ReducerCollectorSearch();
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
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
		
}
