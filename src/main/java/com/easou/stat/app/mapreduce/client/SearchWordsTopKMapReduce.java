package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.SearchDimensions;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TreeMapComparator;
import com.easou.stat.app.mapreduce.core.TreeMapKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorSearchWordTop;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: SearchWordsTopKMapReduce.java
 * @Description: 搜索词榜单MR
 * @author: Asa
 * @date: 2014年5月13日 下午4:37:00
 *
 */
public class SearchWordsTopKMapReduce extends Configured implements Tool {

	public static final Log LOG_MR = LogFactory
			.getLog(SearchWordsTopKMapReduce.class);
	
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
		DBOutputFormat.setOutput(job, ReducerCollectorSearchWordTop
				.getTableNameByGranularity(op.getGranularity()),
				ReducerCollectorSearchWordTop.getFields());

		job.setMapperClass(SearchWordsTopKMapper.class);
		job.setReducerClass(SearchWordsTopKReduecer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		job.setOutputKeyClass(ReducerCollectorSearchWordTop.class);
		job.setOutputValueClass(NullWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class SearchWordsTopKMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		public int K;
		private static final Map<String, TreeMap<TreeMapKey, String>> topMap = new HashMap<String, TreeMap<TreeMapKey, String>>();

		private static final Text result = new Text();

		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString()))
				return;
			JSONObject jobj = JSONObject.parseObject(_line.toString());
			String appkey = jobj.getString("appkey");
			String phone_softversion = jobj.getString("phone_softversion");
			String indicator = jobj.getString("indicator");
			int value = jobj.getInteger("value");
			if(value <= 0)
				return;
			TreeMapKey mapKey = new TreeMapKey(value, jobj.getString("searchWord"));
			String topMapKey = appkey + Constant.DELIMITER + phone_softversion
					+ Constant.DELIMITER + indicator;

			TreeMap<TreeMapKey, String> fatcats = topMap.get(topMapKey);

			if (fatcats == null) {
				fatcats = new TreeMap<TreeMapKey, String>(new TreeMapComparator());
				fatcats.put(mapKey, jobj.toJSONString());
				if (fatcats.size() > K)
					fatcats.remove(fatcats.firstKey());
				topMap.put(topMapKey, fatcats);
			} else {
				fatcats.put(mapKey, jobj.toJSONString());
				if (fatcats.size() > K)
					fatcats.remove(fatcats.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (TreeMap<TreeMapKey, String> fatcats : topMap.values()) {
				for (TreeMapKey key : fatcats.keySet()) {
					String treeMapText = fatcats.get(key);
					if(StringUtils.isNotBlank(treeMapText)){
						result.set(treeMapText);
						context.write(NullWritable.get(), result);
					}
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration config = context.getConfiguration();
			K = config.getInt("rank.max.nums", 1000);
		}

	}

	public static class SearchWordsTopKReduecer
			extends
			Reducer<NullWritable, Text, ReducerCollectorSearchWordTop, NullWritable> {
		public int K;
		private String jobId = null;
		private static final Map<String, TreeMap<TreeMapKey, String>> topMap = new HashMap<String, TreeMap<TreeMapKey, String>>();

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				JSONObject jobj = JSONObject.parseObject(value.toString());
				String appkey = jobj.getString("appkey");
				String phone_softversion = jobj.getString("phone_softversion");
				String indicator = jobj.getString("indicator");
				int num = jobj.getInteger("value");
				
				TreeMapKey mapKey = new TreeMapKey(num, jobj.getString("searchWord"));
				String topMapKey = appkey + Constant.DELIMITER
						+ phone_softversion + Constant.DELIMITER + indicator;

				TreeMap<TreeMapKey, String> fatcats = topMap.get(topMapKey);

				if (fatcats == null) {
					fatcats = new TreeMap<TreeMapKey, String>(new TreeMapComparator());
					fatcats.put(mapKey, jobj.toJSONString());
					if (fatcats.size() > K)
						fatcats.remove(fatcats.firstKey());
					topMap.put(topMapKey, fatcats);
				} else {
					fatcats.put(mapKey, jobj.toJSONString());
					if (fatcats.size() > K)
						fatcats.remove(fatcats.firstKey());
				}
			}
			ReducerCollectorSearchWordTop outData = new ReducerCollectorSearchWordTop();
			outData.setJobid(jobId);
			for (TreeMap<TreeMapKey, String> fatcats : topMap.values()) {
				for (TreeMapKey rkey : fatcats.keySet()) {
					String treeMapText = fatcats.get(rkey);
					if(StringUtils.isNotBlank(treeMapText)){
						JSONObject jobj = JSONObject.parseObject(treeMapText);
						String appkey = jobj.getString("appkey");
						String phone_softversion = jobj.getString("phone_softversion");
						String stat_date = jobj.getString("stat_date");
						String time_peroid = jobj.getString("time_peroid");
						String searchWord = jobj.getString("searchWord");
						String indicator = jobj.getString("indicator");
						float stimes = jobj.getFloat(SearchDimensions.stimes.toString());
						float s1times = jobj.getFloat(SearchDimensions.s1times.toString());
						float s0times = jobj.getFloat(SearchDimensions.s0times.toString());
						float sact1times = jobj.getFloat(SearchDimensions.sact1times.toString());
						float sact0times = jobj.getFloat(SearchDimensions.sact0times.toString());
						
						outData.setAppkey(appkey);
						outData.setPhone_softversion(phone_softversion);
						outData.setStat_date(stat_date);
						outData.setTime_peroid(time_peroid);
						outData.setDim_code(searchWord);
						outData.setIndicator(indicator);
						outData.setValue_stimes(stimes);
						outData.setValue_s1times(s1times);
						outData.setValue_s0times(s0times);
						outData.setValue_sact1times(sact1times);
						outData.setValue_sact0times(sact0times);
						context.write(outData, NullWritable.get());
					}
					
					
				}
			}

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration config = context.getConfiguration();
			K = config.getInt("rank.max.nums", 100);
			this.jobId = context.getJobID().toString();
		}

	}

}
