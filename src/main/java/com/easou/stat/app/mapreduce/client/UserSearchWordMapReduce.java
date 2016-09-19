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

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorUserSearchWord;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;
import com.easou.stat.app.util.MRDriver;

public class UserSearchWordMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(UserSearchWordMapReduce.class);

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
		DBOutputFormat.setOutput(job, ReducerCollectorUserSearchWord.getTableNameByGranularity(), ReducerCollectorUserSearchWord.getFields());

		job.setMapperClass(UserSearchWordMapper.class);
		job.setReducerClass(UserSearchWordReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setOutputKeyClass(ReducerCollectorUserSearchWord.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class UserSearchWordMapper extends Mapper<LongWritable, Text, TextKey, Text> {
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
			String version = (String) lineMap.get("phone_softversion");
			String udid = (String) lineMap.get("phone_udid");
			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, version, false);
			dims = composeDims(dims, dateTime, false);
			dims = composeDims(dims, peroid, false);
			dims = composeDims(dims, udid, false);
			dims = composeDims(dims, searchWord, false);
			for(String dim : dims) {
				write(context, dim, "1");
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
			System.out.println(dateTime);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			try {
				this.time = sdf.parse(dateTime).getTime();
			} catch (ParseException e) {
				LOG_MR.error(e);
			}
		}
	}

	public static class UserSearchWordReducer extends Reducer<TextKey, Text, ReducerCollectorUserSearchWord, NullWritable> {
		private String jobId = null;
		protected void reduce(TextKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println(key.getDimension().toString());
			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String version = keyStr[1];
			String dateTime = keyStr[2];
			String peroid = keyStr[3];
			String dim = "phone_udid";
			String dimCode = keyStr[4];
			String indicator = keyStr[5];
			float result = 0.0f;
			int count = 0;
			for(Text value : values) {
				count ++;
			}
			result = count;
			ReducerCollectorUserSearchWord outData = new ReducerCollectorUserSearchWord();
			outData.setAppkey(appKey);
			outData.setDim_type(dim);
			outData.setDim_code(dimCode);
			outData.setIndicator(indicator);
			outData.setJobid(this.jobId);
			outData.setPhone_softversion(version);
			outData.setStat_date(dateTime);
			outData.setTime_peroid(peroid);
			outData.setValue(result);
			System.out.println(outData);
			context.write(outData, NullWritable.get());
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
	
}
