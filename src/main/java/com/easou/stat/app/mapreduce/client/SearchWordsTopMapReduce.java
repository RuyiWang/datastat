package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.nutz.json.Json;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorSearchWord;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;

public class SearchWordsTopMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(SearchWordsTopMapReduce.class);

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
		DBOutputFormat.setOutput(job, ReducerCollectorSearchWord.getTableNameByGranularity(op.getGranularity()), ReducerCollectorSearchWord.getFields());

		job.setMapperClass(SearchWordsTopMapper.class);
		job.setReducerClass(SearchWordsTopReducer.class);

		job.setPartitionerClass(HashPartitioner.class);
		job.setGroupingComparatorClass(Text.Comparator.class);
		
		job.setOutputKeyClass(ReducerCollectorSearchWord.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class SearchWordsTopMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String granularity = null;
		private long time = 0L;

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
			Integer pageId = (Integer) lineMap.get("page_id");
			if(pageId == null || pageId > 1) {
				return;
			}
			String searchWord = (String) lineMap.get("searchword");
			searchWord = StringUtils.trimToEmpty(searchWord);
			if("".equals(searchWord))
				return;
			searchWord = searchWord.toLowerCase();
			if(checkWordLength(searchWord))
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
			String appkey = (String) lineMap.get("appkey");
			//String version = (String) lineMap.get("phone_softversion");
			List<String> dims = composeDims(null, appkey, false);
			//dims = composeDims(dims, version, true);
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

	public static class SearchWordsTopReducer extends Reducer<Text, Text, ReducerCollectorSearchWord, NullWritable> {
		private String jobId = null;
		@SuppressWarnings("unchecked")
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] keyStr = key.toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			//String version = keyStr[1];
			String dateTime = keyStr[1];
			String peroid = keyStr[2];
			String searchWord = keyStr[3];
			//总搜索次数
			int totalCnt = 0;
			//失败搜索次数
			int failureCnt = 0;
			//主动搜索次数
			int actCnt = 0;
			for(Text value : values) {
				Map<String, Object> lineMap = Json.fromJson(Map.class, value.toString());
				if(isNoResult(lineMap))
					failureCnt ++;
				String searchAction = (String) lineMap.get("searchaction");
				if("1".equals(searchAction))
					actCnt ++;
				totalCnt ++;
			}
			ReducerCollectorSearchWord outData = new ReducerCollectorSearchWord();
			outData.setAppkey(appKey);
			outData.setJobid(this.jobId);
			outData.setPhone_softversion("all");
			outData.setStat_date(dateTime);
			outData.setTime_peroid(peroid);
			outData.setDim_code(searchWord);
//			stimes:搜索次数
//			s1times:成功次数
//			s0times:失败次数
//			sact1times:主动搜索次数
//			sact0times:被动搜索次数
			outData.setIndicator("stimes");
			outData.setValue((float)totalCnt);
			context.write(outData, NullWritable.get());
			outData.setIndicator("s1times");
			outData.setValue((float)(totalCnt - failureCnt));
			context.write(outData, NullWritable.get());
			outData.setIndicator("s0times");
			outData.setValue((float)failureCnt);
			context.write(outData, NullWritable.get());
			outData.setIndicator("sact1times");
			outData.setValue((float)actCnt);
			context.write(outData, NullWritable.get());
			outData.setIndicator("sact0times");
			outData.setValue((float)(totalCnt - actCnt));
			context.write(outData, NullWritable.get());
		}
		
		
		@SuppressWarnings("unchecked")
		private boolean isNoResult(Map<String, Object> lineMap) {
			Map<String, Integer> ss = (Map<String, Integer>) lineMap.get("searchresults");
			boolean isNoResult = true;
			Set<String> keys = ss.keySet();
			for(String key : keys) {
				if(ss.get(key) != 0) {
					isNoResult = false;
					break;
				}
			}
			return isNoResult;
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
	
}
