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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.nutz.json.Json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.constant.SearchDimensions;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: SearchWordsMapReduce.java
 * @Description: 搜索词分析MR step-1
 * @author: Asa
 * @date: 2014年5月13日 下午4:36:01
 *
 */
public class SearchWordsMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(SearchWordsMapReduce.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "false"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "false");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map), false);

		Job job = op.getJob();
		String path = "/runtime/tmp/client/"
				+ CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/")
				+ java.util.UUID.randomUUID().toString();
		FileInputFormat.setInputPaths(job, op.getPaths());
		FileOutputFormat.setOutputPath(job, new Path(path));

		job.setMapperClass(SearchWordsTopMapper.class);
		job.setReducerClass(SearchWordsTopReducer.class);

		job.setPartitionerClass(HashPartitioner.class);
		job.setGroupingComparatorClass(Text.Comparator.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		LOG_MR.info("预处理数据的输出路径为" + path);
		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			op.execute(args[0], path);
		}

		return 0;

	}
	

	public static class SearchWordsTopMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String granularity = null;
		private long time = 0L;

		@Override
		protected void map(LongWritable l, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			String dateFormat = "yyyyMMddHHmmss";
			Date date = null;
			
			JSONObject lineMap = null;
			try{
				lineMap = JSONObject.parseObject(_line.toString());
			}catch(Exception e){
				return;
			}
			if(lineMap == null)
				return;
			
			Integer pageId = (Integer) lineMap.getInteger("page_id");
			if(pageId == null || pageId > 1) {
				return;
			}
			String searchWord = lineMap.getString("searchword");
			searchWord = StringUtils.trimToEmpty(searchWord);
			if(StringUtils.isBlank(searchWord))
				return;
			searchWord = searchWord.toLowerCase();
			if(checkWordLength(searchWord))
				return;
			if(StringUtils.isBlank(lineMap.getString("appkey")))
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
			String appkey =  lineMap.getString("appkey");
			String version =  lineMap.getString("phone_softversion");
			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, version, true);
			dims = composeDims(dims, dateTime, false);
			dims = composeDims(dims, peroid, false);
			dims = composeDims(dims, searchWord, false);
			for(String dim : dims) {
				context.write(new Text(dim), _line);
			}
		}
		
		private boolean checkWordLength(String word) {
			try {
				return word.getBytes("UTF-8").length > 1024;
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return true;
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

	public static class SearchWordsTopReducer extends Reducer<Text, Text, Text, NullWritable> {
		private final Text result = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] keyStr = key.toString().split(Constant.DELIMITER_REG);
			String appkey = keyStr[0];
			String phone_softversion = keyStr[1];
			String stat_date = keyStr[2];
			String time_peroid = keyStr[3];
			String searchWord = keyStr[4];

			//总搜索次数
			float totalCnt = 0f;
			//失败搜索次数
			float failureCnt = 0f;
			//主动搜索次数
			float actCnt = 0f;
			
			float forbiddenCnt = 0f;
			
			for(Text value : values) {
				JSONObject lineMap = JSON.parseObject(value.toString());
				if(isNoResult(lineMap))
					failureCnt ++;
				String searchAction = lineMap.getString("searchaction");
				if("1".equals(searchAction))
					actCnt ++;
				if(isForbidden(lineMap))
					forbiddenCnt++;
				totalCnt ++;
			}
			JSONObject jobj = new JSONObject();
			jobj.put("appkey", appkey);
			jobj.put("phone_softversion", phone_softversion);
			jobj.put("stat_date", stat_date);
			jobj.put("time_peroid", time_peroid);
			jobj.put("searchWord", searchWord);
			jobj.put(SearchDimensions.stimes.toString(),totalCnt);
			jobj.put(SearchDimensions.s1times.toString(),(totalCnt-failureCnt));
			jobj.put(SearchDimensions.s0times.toString(),failureCnt);
			jobj.put(SearchDimensions.sact1times.toString(),actCnt);
			jobj.put(SearchDimensions.sact0times.toString(),(totalCnt-actCnt));
			
			
			jobj.put("indicator", SearchDimensions.stimes.toString());
			jobj.put("value", totalCnt);
			result.set(jobj.toJSONString());
			context.write(result, NullWritable.get());
			
			jobj.put("indicator", SearchDimensions.s1times.toString());
			jobj.put("value", (totalCnt-failureCnt));
			result.set(jobj.toJSONString());
			context.write(result, NullWritable.get());
			
			jobj.put("indicator", SearchDimensions.s0times.toString());
			jobj.put("value", failureCnt);
			result.set(jobj.toJSONString());
			context.write(result, NullWritable.get());
			
			jobj.put("indicator", SearchDimensions.sact1times.toString());
			jobj.put("value", actCnt);
			result.set(jobj.toJSONString());
			context.write(result, NullWritable.get());
			
			
			jobj.put("indicator", SearchDimensions.sact0times.toString());
			jobj.put("value", (totalCnt-actCnt));
			result.set(jobj.toJSONString());
			context.write(result, NullWritable.get());
			
			if(forbiddenCnt > 0 ){
				jobj.put("indicator", SearchDimensions.forbiddens.toString());
				jobj.put("value", forbiddenCnt);
				result.set(jobj.toJSONString());
				context.write(result, NullWritable.get());
			}
		}
		
		
		private boolean isNoResult(JSONObject lineMap) {
			int ss =  lineMap.getInteger("searchresults");
			boolean isNoResult = true;
			if(ss > 0) {
				isNoResult = false;
			}
			return isNoResult;
		}
		/**
		 * 
		 * @Titile: isForbidden
		 * @Description: 
		 * @author: Asa
		 * @date: 2014年4月29日 下午6:04:20
		 * @return boolean
		 */
		private boolean isForbidden(JSONObject lineMap) {
			String isForbid =  lineMap.getString("isForbid");
			boolean isNoResult = false;
			if("true".equals(isForbid)) {
				isNoResult = true;
			}
			return isNoResult;
		}

	}
	
}
