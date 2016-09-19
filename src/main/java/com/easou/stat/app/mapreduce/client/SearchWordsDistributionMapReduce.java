package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.SearchDimensions;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorSearchWord;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: SearchWordsDistributionMapReduce.java
 * @Description: 搜索词分布MR
 * @author: Asa
 * @date: 2014年5月13日 下午4:36:30
 *
 */
public class SearchWordsDistributionMapReduce extends Configured implements
		Tool {
	public static final Log LOG_MR = LogFactory
			.getLog(SearchWordsDistributionMapReduce.class);

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
		DBOutputFormat.setOutput(job, ReducerCollectorSearchWord
				.getTableNameByGranularity(op.getGranularity()),
				ReducerCollectorSearchWord.getFields());

		job.setMapperClass(SearchWordsDistributionMapper.class);
		job.setReducerClass(SearchWordsDistributionReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setOutputKeyClass(ReducerCollectorSearchWord.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		return (job.waitForCompletion(true) ? 0 : 1);

	}

	public static class SearchWordsDistributionMapper extends
			Mapper<LongWritable, Text, TextKey, Text> {

		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString()))
				return;
			JSONObject jobj = null;
			try{
				jobj = JSONObject.parseObject(_line.toString());
			}catch(Exception e){
				return;
			}
			if(jobj == null)
				return;
			
			String appkey = jobj.getString("appkey");
			String phone_softversion = jobj.getString("phone_softversion");
			String stat_date = jobj.getString("stat_date");
			String time_peroid = jobj.getString("time_peroid");
			String searchWord = jobj.getString("searchWord");
			String indicator = jobj.getString("indicator");
			float value = jobj.getFloat("value");

			if (!SearchDimensions.stimes.toString().equalsIgnoreCase(indicator))
				return;
			String stimes = getStimesRange(value);
			writeByDim(appkey + Constant.DELIMITER + phone_softversion
					+ Constant.DELIMITER + stat_date + Constant.DELIMITER
					+ time_peroid + Constant.DELIMITER + indicator
					+ Constant.DELIMITER + stimes, searchWord, context);
		}

		private void writeByDim(String key, String searchWord, Context context)
				throws IOException, InterruptedException {

			Text textValue = new Text(searchWord);
			TextKey textKey = new TextKey(new Text(key), new Text(searchWord));
			context.write(textKey, textValue);
		}

		private String getStimesRange(float stimes) {

			if (stimes <= 10) {
				return "1";
			} else if (stimes <= 40) {
				return "2";
			} else if (stimes <= 100) {
				return "3";
			} else if (stimes <= 200) {
				return "4";
			} else if (stimes <= 1000) {
				return "5";
			} else {
				return "6";
			}
		}

	}

	public static class SearchWordsDistributionReducer extends
			Reducer<TextKey, Text, ReducerCollectorSearchWord, NullWritable> {
		private String jobId = null;

		@Override
		protected void reduce(TextKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String[] keyStr = key.getDimension().toString()
					.split(Constant.DELIMITER_REG);
			String appKey = keyStr[0];
			String version = keyStr[1];
			String dateTime = keyStr[2];
			String time_peroid = keyStr[3];
			String indicator = keyStr[4];
			String stimes = keyStr[5];

			ReducerCollectorSearchWord outData = new ReducerCollectorSearchWord();
			outData.setJobid(jobId);
			outData.setAppkey(appKey);
			outData.setPhone_softversion(version);
			outData.setStat_date(dateTime);
			outData.setTime_peroid(time_peroid);
			outData.setDim_code(stimes);
			outData.setIndicator(indicator);

			float count = 0f;
			for (Text value : values) {
				count++;
			}
			outData.setValue(count);
			context.write(outData, NullWritable.get());

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.jobId = context.getJobID().toString();
		}

	}

}
