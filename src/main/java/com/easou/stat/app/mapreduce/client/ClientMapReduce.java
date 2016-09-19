package com.easou.stat.app.mapreduce.client;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.util.Tool;
import org.nutz.json.Json;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.IntKey;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;
/**
 * 
 * @ClassName: ClientMapReduce.java
 * @Description: 此MR已经废弃
 * @author: Asa
 * @date: 2014年6月5日 上午10:59:50
 *
 */
public class ClientMapReduce extends Configured implements Tool {
	public static final Log LOG_MR = LogFactory.getLog(ClientMapReduce.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);
		map.put(Constant.APP_TMP_MAPREDUCE, "true"); // 有后续MR执行标识
		map.put(Constant.APP_MOBILEINFO_MAPREDUCE, "true"); // 客户端手机信息数据
		map.put(Constant.PHONE_BRAND, "true");
		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);
		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();
		String path = "/runtime/tmp/client/"
				+ CommonUtils.timeToString(new Date(), "yyyyMMdd/HHmm/")
				+ java.util.UUID.randomUUID().toString();
		FileInputFormat.setInputPaths(job, op.getPaths());
		FileOutputFormat.setOutputPath(job, new Path(path));

		job.setMapperClass(ClientMapper.class);
		job.setReducerClass(ClientReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(IntKey.GroupingComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(IntKey.class);
		job.setMapOutputValueClass(Text.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");
		LOG_MR.info("预处理数据的输出路径为" + path);

		if (job.waitForCompletion(true)) {
			map.remove(Constant.APP_TMP_MAPREDUCE);
			op.execute(args[0], path);
		}

		return 0;
		// return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ClientMapper extends
			Mapper<LongWritable, Text, IntKey, Text> {
		@SuppressWarnings("unused")
		@Override
		protected void map(LongWritable l, Text _line, Context context)
				throws IOException, InterruptedException {
			if (StringUtils.isBlank(_line.toString())) {
				return;
			}
			AppLogFormat _l = new AppLogFormat(_line.toString());

			if (_l == null) {
				return;
			}
			if (_l.isMobilePhoneInfo()) {
				MapReduceHelpler.outputIntKeyValue(
						_l.getOpenudid() + _l.getAppkey(),
						Constant.INT_COUNT_TWO, _line.toString(), context);
			}
			if (_l.isBehaviorInfo()) {
				MapReduceHelpler.outputIntKeyValue(
						_l.getOpenudid() + _l.getAppkey(),
						Constant.INT_COUNT_ZERO, _line.toString(), context);
			}

			if (_l.isActivityInfo()) {
				MapReduceHelpler.outputIntKeyValue(
						_l.getOpenudid() + _l.getAppkey(),
						Constant.INT_COUNT_ONE, _line.toString(), context);
			}

		}
	}

	public static class ClientReducer extends
			Reducer<IntKey, Text, Text, NullWritable> {
		Map<String, String> map = new HashMap<String, String>();

		@SuppressWarnings("unchecked")
		protected void reduce(IntKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Object> mapMValue = new HashMap<String, Object>();//手机信息数据
			Map<String, Object> mapBValue = new HashMap<String, Object>();//手机行为数据
			Map<String, Object> mapAValue = new HashMap<String, Object>();//用户访问日志
			// 处理客户端用户信息有多条的情况(只取一条)
			double i = 0;
			for (Text v : values) {
				if (key.getValue().get() == Constant.INT_COUNT_TWO && i == 0) {
					mapMValue = Json.fromJson(Map.class, v.toString());
					// 去掉用户信息和用户行为日志重复的键，以用户行为日志的为准
					mapMValue.remove("type");
					mapMValue.remove("wifi");
					mapMValue.remove("cpid");
					mapMValue.remove("currentnetworktype");
					//mapMValue.remove("phone_softversion");
					// mapMValue.remove("appkey");
					// mapMValue.remove("phone_esid");
					mapMValue.remove("phone_imei");
					mapMValue.remove("phone_udid");
					mapMValue.remove("phone_mac");
					mapMValue.remove("phone_imsi");
					mapMValue.remove("phone_city");
					String result = CommonUtils.convertModelsToBrand(map,
							(String) mapMValue.get("phone_model"));
					if (null != result && !result.equals("")) {
						mapMValue.put("phone_model", result);
					}
					i++;
				}
				// 如果没有用户客户端日志，只有用户行为日志，把用户行为日志废除
				if (mapMValue.size() > 0) {
					if (key.getValue().get() == Constant.INT_COUNT_ZERO) {
						mapMValue.remove("phone_softversion");
						mapBValue = Json.fromJson(Map.class, v.toString());
						// 如果行为日志里phone_apn这个属性不为空，取行为里的，否则取用户信息里的 
						if (StringUtils.isNotEmpty(String.valueOf(mapBValue
								.get("phone_apn")))) {
							mapMValue.remove("phone_apn");
						}
						mapBValue.putAll(mapMValue);
						context.write(
								new Text(Json.toJson(mapBValue,
										Constant.JSON_FORMAT)), NullWritable
										.get());
					} else if (key.getValue().get() == Constant.INT_COUNT_ONE) {
						mapAValue = Json.fromJson(Map.class, v.toString());
						mapAValue.putAll(mapMValue);
						context.write(
								new Text(Json.toJson(mapAValue,
										Constant.JSON_FORMAT)), NullWritable
										.get());
					}
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			String phoneBrands = context.getConfiguration().get(
					Constant.PHONE_BRAND);
			if (null != phoneBrands && !phoneBrands.equals("")) {
				map = (Map<String, String>) Json.fromJson(phoneBrands);
			}
		}
	}
}
