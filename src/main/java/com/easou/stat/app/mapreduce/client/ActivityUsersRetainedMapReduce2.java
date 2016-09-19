package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.mrDBEntity.ReducerCollectorUsersRetained;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ActivityUsersRetainedMapReduce1.java
 * @Description: 新用户留存计算-step2
 * @author: Asa
 * @date: 2014年6月5日 上午11:02:07
 *
 */
public class ActivityUsersRetainedMapReduce2 extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ActivityUsersRetainedMapReduce2.class);

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
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorUsersRetained.getTableNameByGranularity(), ReducerCollectorUsersRetained.getFields());
		job.setOutputFormatClass(OracleDBOutputFormat.class);
		
		job.setMapperClass(ActivityUsersRetained2Mapper.class);
		job.setReducerClass(ActivityUsersRetained2Reducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);
		
		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ReducerCollectorUsersRetained.class);
		job.setOutputValueClass(NullWritable.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ActivityUsersRetained2Mapper extends
			Mapper<LongWritable, Text, TextKey, Text> {
		@Override
		protected void map(LongWritable key, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			JSONObject lineMap = JSONObject.parseObject(_line.toString());
			if (lineMap == null)
				return;
			String appkey = lineMap.getString("appkey");
			String version = lineMap.getString("phone_softversion");
			String indicator = lineMap.getString("indicator");
			String stat_date = lineMap.getString("server_time")+"000000";
			
			List<String> dims = composeDims(null, appkey, false);
			dims = composeDims(dims, version, true);
			dims = composeDims(dims, stat_date, false);
			// cpid
			String cpid = (String) lineMap.get("cpid");
			if (cpid == null || "".equals(cpid)) {
				return;
			}
			if (cpid.length() > 24) {
				cpid = cpid.substring(0, 24);
			}
			dims = composeDims(dims, indicator, false);
			List<String> cpidDims = composeDims(dims, "cpid", cpid, true);
			writeByDim(cpidDims, lineMap, context);
		}

		private List<String> composeDims(List<String> prefixs, String dim,
				boolean isAll) {
			List<String> list = new ArrayList<String>();
			if (prefixs == null || prefixs.size() == 0) {
				list.add(dim);
				if (isAll)
					list.add("all");
			} else {
				for (String prefix : prefixs) {
					list.add(prefix + Constant.DELIMITER + dim);
					if (isAll)
						list.add(prefix + Constant.DELIMITER + "all");
				}
			}
			return list;
		}

		private List<String> composeDims(List<String> prefixs, String dim,
				String dimCode, boolean isAll) {
			List<String> list = new ArrayList<String>();
			dimCode = StringUtils.trimToNull(dimCode);
			if (prefixs == null || prefixs.size() == 0) {
				list.add(dim + Constant.DELIMITER + dimCode);
				if (isAll)
					list.add("all");
			} else {
				for (String prefix : prefixs) {
					if (StringUtils.isNotEmpty(dimCode))
						list.add(prefix + Constant.DELIMITER + dim
								+ Constant.DELIMITER + dimCode);
					if (isAll)
						list.add(prefix + Constant.DELIMITER + dim
								+ Constant.DELIMITER + "all");
				}
			}
			return list;
		}

		private void writeByDim(List<String> dims, JSONObject lineMap,
				Context context) throws IOException, InterruptedException {

			for (String dim : dims) {
				write(context, dim, lineMap.getString("phone_udid"));
			}
		}

		private void write(Context context, String key, String value)
				throws IOException, InterruptedException {
			Text textValue = new Text(value);
			TextKey textKey = new TextKey(new Text(key), textValue);
			context.write(textKey, textValue);
		}

	}

	public static class ActivityUsersRetained2Reducer extends
			Reducer<TextKey, Text, ReducerCollectorUsersRetained, NullWritable> {
		private String jobId = null;
		@Override
		protected void reduce(TextKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String[] keyStr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			String appkey = keyStr[0];
			String version = keyStr[1];
			String stat_date = keyStr[2];
			String indicator = keyStr[3];
			String dim_type = keyStr[4];
			String dim_code = keyStr[5];
			
			
			int count = 0;
			for (Text value : values) {
					count++;
			}
			ReducerCollectorUsersRetained result = new ReducerCollectorUsersRetained();
			result.setAppkey(appkey);
			result.setDim_code(dim_code);
			result.setDim_type(dim_type);
			result.setIndicator(indicator);
			result.setJobid(jobId);
			result.setPhone_softversion(version);
			result.setStat_date(stat_date);
			result.setTime_peroid("1");
			result.setValue(count);
			context.write(result, NullWritable.get());
			
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.jobId = context.getJobID().toString();
		}
	}
	
	public static void main(String[] args){
		
	}
	
}
