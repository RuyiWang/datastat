package com.easou.stat.app.mapreduce.define;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

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
import com.easou.stat.app.constant.Dimensions;
import com.easou.stat.app.mapreduce.core.AppLogFormat;
import com.easou.stat.app.mapreduce.core.Behavior;
import com.easou.stat.app.mapreduce.core.FirstPartitioner;
import com.easou.stat.app.mapreduce.core.MapReduceHelpler;
import com.easou.stat.app.mapreduce.core.OpUnit;
import com.easou.stat.app.mapreduce.core.OracleDBOutputFormat;
import com.easou.stat.app.mapreduce.core.TextKey;
import com.easou.stat.app.mapreduce.po.ReducerCollectorDefine;
import com.easou.stat.app.mapreduce.util.SurvivalTimeComparator;
import com.easou.stat.app.mapreduce.util.SurvivalTimeUnit;
import com.easou.stat.app.mapreduce.util.SurvivalTimeUtil;
import com.easou.stat.app.schedule.db.UserDefinedEvent;
import com.easou.stat.app.util.AnalysisAttr;
import com.easou.stat.app.util.AnalysisAttr.ConditionUnit;
import com.easou.stat.app.util.AnalysisAttr.ResultBean;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.MRDriver;

/**
 * @ClassName: ClientUserDefinedEventMapReduce
 * @Description: 用户自定义事件
 * @author vic
 * @date Oct 16, 2012 4:27:31 PM
 * 
 */
public class ClientUserDefinedEventMapReduce extends Configured implements Tool {
	public static final Log	LOG_MR	= LogFactory.getLog(ClientUserDefinedEventMapReduce.class);

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, args[0]);

		String code = java.util.UUID.randomUUID().toString();
		map.put("任务标志", code);

		MRDriver op = new MRDriver(getConf(), Json.toJson(map));

		Job job = op.getJob();
		FileInputFormat.setInputPaths(job, op.getPaths());
		DBOutputFormat.setOutput(job, ReducerCollectorDefine.getTableNameByGranularity(op.getGranularity()), ReducerCollectorDefine.getFields());

		job.setMapperClass(ClientUserDefinedEventMapper.class);
		job.setReducerClass(ClientUserDefinedEventReducer.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(TextKey.GroupingComparator.class);

		job.setMapOutputKeyClass(TextKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ReducerCollectorDefine.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(OracleDBOutputFormat.class);

		LOG_MR.info("预处理数据的MapReduce驱动配置完成!");
		LOG_MR.info("预处理数据的MapReduce任务准备提交!");

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class ClientUserDefinedEventMapper extends Mapper<LongWritable, Text, TextKey, Text> {
		private boolean				dim_type_city				= Boolean.FALSE;
		private boolean				dim_type_cpid				= Boolean.FALSE;
		private boolean				dim_type_version			= Boolean.FALSE;
		private boolean				dim_type_phone_model		= Boolean.FALSE;
		private boolean				dim_type_phone_apn			= Boolean.FALSE;
		private boolean				dim_type_phone_resolution	= Boolean.FALSE;
		private boolean				dim_type_os					= Boolean.FALSE;
		private boolean				dim_type_version_and_cpid	= Boolean.FALSE;

		/** @Fields SongNumberTag : 计算歌单类指标时，用来传数量值的Key。 **/
		public static final String	SongNumberTag				= "song";
		List<UserDefinedEvent>		list;

		@SuppressWarnings("unused")
		@Override
		protected void map(LongWritable l, Text _line, Context context) throws IOException, InterruptedException {
			AppLogFormat _l = new AppLogFormat(_line.toString());

			if (_l == null) {
				return;
			}

			// UDID
			String udid = _l.getOpenudid();

			List<Behavior> behaviors = _l.getBehaviors();

			AnalysisAttr a = new AnalysisAttr();
			if (null != list && null != behaviors) {
				for (UserDefinedEvent ude : list) {
					// 表达式
					String express = ude.getExpress();
					// eventId
					String eventId = ude.getEventId();
					Vector<ResultBean> v = a.doTask(express);
					for (int j = 0; j < v.size(); j++) {
						ResultBean rb = v.get(j);
						if (null != rb && CommonUtils.isNumeric(rb.getRule())) {
							Integer rule = Integer.valueOf(rb.getRule());
							// 自定义事件属性
							switch (rule) {
							case Constant.rule_1:
							case Constant.rule_2:
								for (Behavior b : behaviors) {
									OpUnit op = b.parseOp();
									// 指标
									String indicator = rb.getTarget();
									if (null != op && eventId.equals(op.getEventID())) {
										// 条件
										Vector<List<ConditionUnit>> cd = rb.getConditionsList();
										if (null != cd && !cd.isEmpty()) {
											// 设置了条件的事件日志肯定是有属性的
											if (b.getData() == 1) {
												Map<String, String> properties = b.getPropertiesValue();
												if (null != properties && !properties.isEmpty()) {
													for (List<ConditionUnit> c : cd) {
														if (null != c) {
															List<ConditionUnit> c1 = new ArrayList<ConditionUnit>();
															boolean flag = Boolean.TRUE;
															if (null != c && !c.isEmpty()) {
																for (ConditionUnit conditionUnit : c) {
																	boolean tmp = Boolean.FALSE;
																	if (properties.containsKey(conditionUnit.getKey())) {
																		if (properties.get(conditionUnit.getKey()).equals(conditionUnit.getValue()) || Constant.ALL.equals(conditionUnit.getValue())) {
																			tmp = Boolean.TRUE;
																		}
																	}
																	flag = flag & tmp;
																	if (!flag) {
																		break;
																	}
																}
																if (flag) {
																	String isTop = Constant.isNotTop;
																	for (ConditionUnit conditionUnit : c) {
																		if (Constant.ALL.equals(conditionUnit.getValue())) {
																			ConditionUnit cond = new ConditionUnit(conditionUnit.getKey(), properties.get(conditionUnit.getKey()));
																			c1.add(cond);
																			isTop = Constant.isTop;
																		} else {
																			c1.add(conditionUnit);
																		}
																	}
																	outputKeyValue(_l, udid, Constant.ONE, context, eventId, indicator, Json.toJson(c1), rule, isTop);
																}
															}
														}
													}
												}
											}
										} else {
											outputKeyValue(_l, udid, Constant.ONE, context, eventId, indicator, " ", rule, Constant.isNotTop);
										}
									}
								}
								break;
							case Constant.rule_3:
								// traffic
								String traffic = _l.getTraffic();
								for (Behavior b : behaviors) {
									// 自定义事件只统计是属性的
									OpUnit op = b.parseOp();
									// 指标
									String indicator = rb.getTarget();
									if (null != op && eventId.equals(op.getEventID())) {
										// 条件
										Vector<List<ConditionUnit>> cd = rb.getConditionsList();
										if (null != cd && !cd.isEmpty()) {
											// 设置了条件的事件日志肯定是有属性的
											if (b.getData() == 1) {
												Map<String, String> properties = b.getPropertiesValue();
												if (null != properties && !properties.isEmpty()) {
													for (List<ConditionUnit> c : cd) {
														if (null != c) {
															List<ConditionUnit> c1 = new ArrayList<ConditionUnit>();
															boolean flag = Boolean.TRUE;
															if (null != c && !c.isEmpty()) {
																for (ConditionUnit conditionUnit : c) {
																	boolean tmp = Boolean.FALSE;
																	if (properties.containsKey(conditionUnit.getKey())) {
																		if (properties.get(conditionUnit.getKey()).equals(conditionUnit.getValue()) || Constant.ALL.equals(conditionUnit.getValue())) {
																			tmp = Boolean.TRUE;
																		}
																	}
																	flag = flag & tmp;
																	if (!flag) {
																		break;
																	}
																}
																if (flag) {
																	String isTop = Constant.isNotTop;
																	for (ConditionUnit conditionUnit : c) {
																		if (Constant.ALL.equals(conditionUnit.getValue())) {
																			ConditionUnit cond = new ConditionUnit(conditionUnit.getKey(), properties.get(conditionUnit.getKey()));
																			c1.add(cond);
																			isTop = Constant.isTop;
																		} else {
																			c1.add(conditionUnit);
																		}
																	}
																	outputKeyValue(_l, udid, traffic, context, eventId, indicator, Json.toJson(c1), rule, isTop);
																}
															}
														}
													}
												} else {
													outputKeyValue(_l, udid, traffic, context, eventId, indicator, " ", rule, Constant.isNotTop);
												}
											}
										}
									}
								}
								break;
							case Constant.rule_4:
								for (Behavior b : behaviors) {
									OpUnit op = b.parseOp();
									if ((Constant.ZERO.equals(op.getTimeTag()) || Constant.ONE.equals(op.getTimeTag()))) {
										String indicator = rb.getTarget();

										if (null != op && eventId.equals(op.getEventID())) {
											Vector<List<ConditionUnit>> conditions = rb.getConditionsList();
											if (null != conditions && !conditions.isEmpty()) {
												// 设置了条件的事件日志肯定是有属性的
												if (b.getData() == 1) {
													Map<String, String> properties = b.getPropertiesValue();
													if (null != properties && !properties.isEmpty()) {
														for (List<ConditionUnit> c : conditions) {
															List<ConditionUnit> c1 = new ArrayList<ConditionUnit>();
															boolean flag = Boolean.TRUE;
															if (null != c) {
																for (ConditionUnit conditionUnit : c) {
																	boolean tmp = Boolean.FALSE;
																	if (properties.containsKey(conditionUnit.getKey())) {
																		if (properties.get(conditionUnit.getKey()).equals(conditionUnit.getValue()) || Constant.ALL.equals(conditionUnit.getValue())) {
																			tmp = Boolean.TRUE;
																		}
																	}
																	flag = flag & tmp;
																	if (!flag) {
																		break;
																	}
																}
															}
															if (flag) {
																String isTop = Constant.isNotTop;
																for (ConditionUnit conditionUnit : c) {
																	if (Constant.ALL.equals(conditionUnit.getValue())) {
																		ConditionUnit cond1 = new ConditionUnit(conditionUnit.getKey(), properties.get(conditionUnit.getKey()));
																		c1.add(cond1);
																		isTop = Constant.isTop;
																	} else {
																		c1.add(conditionUnit);
																	}
																}
																outputKeyValue(_l, udid + Constant.DELIMITER + b.getTime() + op.getTimeTag(), "", context, eventId, indicator, Json.toJson(c1), rule, isTop);
															}
														}
													}
												}
											} else {
												outputKeyValue(_l, udid + Constant.DELIMITER + b.getTime() + op.getTimeTag(), "", context, eventId, indicator, " ", rule, Constant.isNotTop);
											}
										}
									}
								}
								break;
							case Constant.rule_5:
								for (Behavior b : behaviors) {
									// 自定义事件只统计是属性的
									if (b.getData() == 1) {
										String value = b.getPropertiesValue().get(SongNumberTag);
										if (value != null) {
											OpUnit op = b.parseOp();
											String indicator = rb.getTarget();
											if (null != op && eventId.equals(op.getEventID())) {
												Vector<List<ConditionUnit>> conditions = rb.getConditionsList();
												if (null != conditions && !conditions.isEmpty()) {
													Map<String, String> properties = b.getPropertiesValue();
													if (null != properties && !properties.isEmpty()) {
														for (List<ConditionUnit> c : conditions) {
															List<ConditionUnit> c1 = new ArrayList<ConditionUnit>();
															boolean flag = Boolean.TRUE;
															if (null != c) {
																for (ConditionUnit conditionUnit : c) {
																	boolean tmp = Boolean.FALSE;
																	if (properties.containsKey(conditionUnit.getKey())) {
																		if (properties.get(conditionUnit.getKey()).equals(conditionUnit.getValue()) || Constant.ALL.equals(conditionUnit.getValue())) {
																			tmp = Boolean.TRUE;
																		}
																	}
																	flag = flag & tmp;
																	if (!flag) {
																		break;
																	}
																}
															}
															if (flag) {
																String isTop = Constant.isNotTop;
																for (ConditionUnit conditionUnit : c) {
																	if (Constant.ALL.equals(conditionUnit.getValue())) {
																		ConditionUnit cond1 = new ConditionUnit(conditionUnit.getKey(), properties.get(conditionUnit.getKey()));
																		c1.add(cond1);
																		isTop = Constant.isTop;
																	} else {
																		c1.add(conditionUnit);
																	}
																}
																outputKeyValue(_l, b.getTime(), value, context, eventId, indicator, Json.toJson(c1), rule, isTop);
															}

														}
													}
												} else {
													outputKeyValue(_l, b.getTime(), value, context, eventId, indicator, " ", rule, Constant.isNotTop);
												}
											}
										}
									}
								}
								break;
							default:
								break;
							}
						}
					}
				}
			}
		}

		/**
		 * @Title: outputKeyValue
		 * @Description: 封装输出键值
		 * @param _l 日志
		 * @param k_str 键
		 * @param v_str 值
		 * @param context
		 * @param eventId 事件ID
		 * @param indicator 指标
		 * @param conditions 条件
		 * @param rule 规则
		 * @throws IOException
		 * @throws InterruptedException
		 * @author vic
		 * @return void
		 * @throws
		 */
		private void outputKeyValue(AppLogFormat _l, String k_str, String v_str, Context context, String eventId, String indicator, String conditions, int rule, String isTop) throws IOException, InterruptedException {
			String appkey = _l.getAppkey();
			String udid = _l.getOpenudid();

			if (StringUtils.isNotEmpty(appkey) && StringUtils.isNotEmpty(udid)) {
				if (dim_type_city) {
					String city = StringUtils.isBlank(_l.getProvince()) ? Constant.OTHER_ZONE : _l.getProvince();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + city
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.city.toString(), context);
				}

				if (dim_type_cpid) {
					// 一级所有
					String cpid = _l.getSalechannel1All();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + cpid
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.cpid.toString(), context);
					// 一级渠道
					cpid = _l.getSalechannel1();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + cpid
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.cpid.toString(), context);
					// 二级渠道
					cpid = _l.getSalechannel2();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + cpid
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.cpid.toString(), context);
				}

				if (dim_type_version) {
					String version = StringUtils.isBlank(_l.getPhoneSoftversion()) ? Constant.OTHER_VERSION : _l.getPhoneSoftversion();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + version
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.version.toString(), context);
				}

				if (dim_type_phone_model) {
					String model = StringUtils.isBlank(_l.getPhoneModel()) ? Constant.OTHER_MODEL : _l.getPhoneModel();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + model
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.phone_model.toString(), context);
				}

				if (dim_type_phone_apn) {
					String apn = StringUtils.isBlank(_l.getPhoneApn()) ? Constant.OTHER_APN : _l.getPhoneApn();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + apn
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.phone_apn.toString(), context);
				}

				if (dim_type_phone_resolution) {
					String resolution = StringUtils.isBlank(_l.getPhoneResolution()) ? Constant.OTHER_RESOLUTION : _l.getPhoneResolution();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + resolution
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.phone_resolution.toString(), context);
				}

				if (dim_type_os) {
					String os = StringUtils.isBlank(_l.getOs()) ? Constant.OTHER_OS : _l.getOs();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + os
							+ Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.os.toString(), context);
				}

				if (dim_type_version_and_cpid) {
					String version = StringUtils.isBlank(_l.getPhoneSoftversion()) ? Constant.OTHER_VERSION : _l.getPhoneSoftversion();
					// 一级所有
					String cpid = _l.getSalechannel1All();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + version
							+ Constant.DELIMITER + cpid + Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.versionAndCpid.toString(), context);
					// 一级渠道
					cpid = _l.getSalechannel1();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + version
							+ Constant.DELIMITER + cpid + Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.versionAndCpid.toString(), context);
					// 二级渠道
					cpid = _l.getSalechannel2();
					// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
					MapReduceHelpler.outputKeyValue(appkey + Constant.DELIMITER + eventId + Constant.DELIMITER + indicator + Constant.DELIMITER + conditions + Constant.DELIMITER + rule + Constant.DELIMITER + version
							+ Constant.DELIMITER + cpid + Constant.DELIMITER + isTop, k_str, v_str + Constant.DELIMITER + Dimensions.versionAndCpid.toString(), context);
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 自定义事件
			String eventList = context.getConfiguration().get(Constant.USER_DEFINED_EVENT_LIST);
			list = Json.fromJsonAsList(UserDefinedEvent.class, eventList);

			Configuration conf = context.getConfiguration();
			List<Dimensions> dimTypes = MRDriver.loadDimTypes(conf);// 加载维度类型
			this.dim_type_city = dimTypes.contains(Dimensions.city);
			this.dim_type_cpid = dimTypes.contains(Dimensions.cpid);
			this.dim_type_version = dimTypes.contains(Dimensions.version);
			this.dim_type_phone_model = dimTypes.contains(Dimensions.phone_model);
			this.dim_type_phone_apn = dimTypes.contains(Dimensions.phone_apn);
			this.dim_type_phone_resolution = dimTypes.contains(Dimensions.phone_resolution);
			this.dim_type_os = dimTypes.contains(Dimensions.os);
			this.dim_type_version_and_cpid = dimTypes.contains(Dimensions.versionAndCpid);
		}
	}

	public static class ClientUserDefinedEventReducer extends Reducer<TextKey, Text, ReducerCollectorDefine, NullWritable> {
		private String	dateTime;
		private String	jobId;

		@SuppressWarnings({ "unchecked" })
		protected void reduce(TextKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// appkey{]eventId{]indicator{]条件{]rule{]dimensions_code{]isTop
			String[] keyArr = key.getDimension().toString().split(Constant.DELIMITER_REG);
			Float value = 0F;
			String dimType = "";

			Integer rule = Integer.valueOf(keyArr[4]);
			switch (rule) {
			case Constant.rule_1:
			case Constant.rule_3:
				for (Text v : values) {
					String[] vStr = v.toString().split(Constant.DELIMITER_REG);
					if (CommonUtils.isNumeric(vStr[0])) {
						value += Float.valueOf(vStr[0]);
					}
					dimType = vStr[1];
				}
				break;
			case Constant.rule_2:
				// 根据udid去重
				String udid = null;
				for (Text v : values) {
					String[] vStr = v.toString().split(Constant.DELIMITER_REG);
					if (!key.getValue().toString().equals(udid)) {
						if (CommonUtils.isNumeric(vStr[0])) {
							value += Float.valueOf(vStr[0]);
						}
					}
					udid = key.getValue().toString();
					dimType = vStr[1];
				}
				break;
			case Constant.rule_4:
				long cursor = 0L;
				List<SurvivalTimeUnit> s = new ArrayList<SurvivalTimeUnit>();
				String udid1 = "";
				for (Text v : values) {
					String[] vStr = v.toString().split(Constant.DELIMITER_REG);
					String[] val = key.getValue().toString().split(Constant.DELIMITER_REG);
					if (!udid1.equals(val[0])) {
						Collections.sort(s, new SurvivalTimeComparator());
						value += SurvivalTimeUtil.getSurvivalTime(s);
						s.clear();
						cursor++;
					}
					s.add(new SurvivalTimeUnit(Long.valueOf(val[1].substring(1, val[1].length() - 1)), val[1].substring(val[1].length() - 1)));
					udid1 = val[0];
					dimType = vStr[1];
				}
				Collections.sort(s, new SurvivalTimeComparator());
				value += SurvivalTimeUtil.getSurvivalTime(s);
				cursor++;
				break;
			case Constant.rule_5:
				int cursor1 = 0;
				// 时间已经经过降序排序，所以第一个则是最近的一个。
				for (Text v : values) {
					String[] vStr = v.toString().split(Constant.DELIMITER_REG);
					if (cursor1 == 0) {
						value = Float.valueOf(vStr[0]);
					}
					cursor1++;
					dimType = vStr[1];
				}
				break;
			default:
				break;
			}

			ReducerCollectorDefine r = new ReducerCollectorDefine();
			r.setJobID(jobId);
			r.setTime(dateTime);
			r.setAppKey(keyArr[0]);
			r.setEventID(keyArr[1]);
			r.setIndicator(keyArr[2]);
			r.setCondition(keyArr[3]);
			r.setDimType(dimType);
			r.setValue(value);
			r.setDimensionsCode(keyArr[5]);
			if (keyArr.length > 7) {
				r.setDimensionsExt1(keyArr[6]);
				r.setIsTop(keyArr[7]);
			} else {
				r.setIsTop(keyArr[6]);
				r.setDimensionsExt1("-");
			}

			context.write(r, NullWritable.get());
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.dateTime = context.getConfiguration().get(Constant.DATETIME);
			this.jobId = context.getJobID().toString();
		}
	}
}
