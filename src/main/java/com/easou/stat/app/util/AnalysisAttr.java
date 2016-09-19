package com.easou.stat.app.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.nutz.json.Json;

import com.easou.stat.app.constant.Constant;

public class AnalysisAttr {
	public static class ResultBean {
		private String target; // 统计
		private String rule; // 规则
		private Vector<String> conditions = new Vector<String>(); // 统计条件

		public String getTarget() {
			return target;
		}

		public void setTarget(String target) {
			this.target = target;
		}

		public String getRule() {
			return rule;
		}

		public void setRule(String rule) {
			this.rule = rule;
		}

		public Vector<String> getConditions() {
			return conditions;
		}

		public void setConditions(Vector<String> conditions) {
			this.conditions = conditions;
		}

		public void addConditions(String condition) {
			this.conditions.add(condition);
		}

		public Vector<List<ConditionUnit>> getConditionsList() {
			Vector<List<ConditionUnit>> conditionsList = new Vector<List<ConditionUnit>>(); // 统计条件(解析后)
			if (null != conditions && !conditions.isEmpty()) {
				for (String c : conditions) {
					List<ConditionUnit> con = parseConditionStr(c);
					conditionsList.add(con);
				}
			}
			return conditionsList;
		}
	}

	public Vector<ResultBean> doTask(String input) {
		Vector<ResultBean> resultBeans = new Vector<ResultBean>();
		if (StringUtils.isNotBlank(input)
				&& input.indexOf("#") > 0
				&& CommonUtils.getSubStrCount(input, "#") == CommonUtils
						.getSubStrCount(input, Constant.DELIMITER_REG) + 1) {
			// 统计指标@统计条件,......统计指标@统计条件
			String[] expresstions = input.split(Constant.DELIMITER_REG);

			if (expresstions != null && expresstions.length > 0) {
				for (int i = 0; i < expresstions.length; i++) {
					// 统计指标@统计条件
					String express = expresstions[i];
					try {
						String[] expressArr = express.split("#");
						if (expressArr != null && expressArr.length > 1) {
							ResultBean resultBean = new ResultBean();
							String target = expressArr[0];
							resultBean.setTarget(target.trim());
							String[] results = expressArr[1].split("@");
							if (results != null) {
								if (results.length == 1) {
									String rule = results[0];
									resultBean.setRule(rule.trim());
								} else if (results.length > 1) {
									String rule = results[0];
									String conditions = results[1];
									resultBean.setRule(rule.trim());
									// 组合关系
									if ((conditions.indexOf("&") > -1 && conditions
											.indexOf("|") > -1)
											|| (conditions.indexOf("|") < 0)) {
										String[] condition_temps = conditions
												.split("\\|");
										if (condition_temps != null
												&& condition_temps.length > 0) {
											for (int j = 0; j < condition_temps.length; j++) {
												resultBean
														.addConditions(condition_temps[j]
																.trim());
											}
										}
									} else if (conditions.indexOf("&") < 0
											&& conditions.indexOf("|") > -1) {
										String[] condition_temps = conditions
												.split("\\|");

										if (condition_temps != null
												&& condition_temps.length > 0) {
											for (int j = 0; j < condition_temps.length; j++) {
												resultBean
														.addConditions(condition_temps[j]
																.trim());
											}
										}

									} else {// 单个条件或者单个组合关系
										resultBean.addConditions(conditions
												.trim());
									}
								}
							}
							resultBeans.add(resultBean);
						}
					} catch (Exception e) {
						System.out.println("下面的表达式不对，请重新配置事件表达式");
						System.out.println(input);
					}
				}
			}
		} else {
			System.out.println("下面的表达式不对，请重新配置事件表达式");
			System.out.println(input);
		}
		return resultBeans;
	}

	/**
	 * @ClassName: ConditionUnit
	 * @Description: 元条件类（对应key=value的情况）。
	 * @author 邓长春
	 * @date 2012-10-17 下午04:53:18
	 * 
	 */
	public static class ConditionUnit {
		private String key;
		private String value;

		public ConditionUnit(String key, String value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	/**
	 * @Title: parseConditionStr
	 * @Description: 子条件进一步的解析方法。
	 * @param str
	 * @return
	 * @author 邓长春
	 * @return List<ConditionUnit>
	 * @throws
	 */
	private static List<ConditionUnit> parseConditionStr(String str) {
		List<ConditionUnit> con = new ArrayList<ConditionUnit>();
		if (StringUtils.isBlank(str)) {
			return null;
		} else {
			String[] conArr = str.split("&");
			if (conArr.length == 0) {
				return null;
			} else {
				for (String e : conArr) {
					String[] eArr = e.split("=");
					if (eArr.length == 2) {
						con.add(new ConditionUnit(eArr[0].trim(), eArr[1]
								.trim()));
					}
				}
				return con;
			}
		}
	}

	public static void main(String[] args) {
		AnalysisAttr a = new AnalysisAttr();
		Vector<ResultBean> r = a
				.doTask("pv#1{]pv#1@channle=1|channel=2|channel=3{]pageView#2@channle=0&channel=8|channel=6{]pageView2#2@channle=0&channel=8&channel=6");
		for (ResultBean bean : r) {
			Vector<List<ConditionUnit>> v = bean.getConditionsList();
			for (List<ConditionUnit> list : v) {
				if (null != list) {
					for (ConditionUnit conditionUnit : list) {
						System.out.println(conditionUnit.getKey() + ":"
								+ conditionUnit.getValue());
					}
				}
			}
			System.out.println(Json.toJson(bean));
			ResultBean rb = Json.fromJson(ResultBean.class, Json.toJson(bean));
		}
	}
}
