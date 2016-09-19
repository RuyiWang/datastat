package com.easou.stat.app.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.nutz.json.Json;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.mapreduce.define.ClientUserDefinedEventMapReduce;
import com.easou.stat.app.schedule.db.UserDefinedEvent;
import com.easou.stat.app.schedule.db.UserDefinedEventDao;

/**
 * @ClassName: PreProcessEngine
 * @Description: 预处理MR数据引擎
 * @author 廖瀚卿
 * @date 2012-7-4 下午02:18:15
 * 
 */
public class PreProcessEngine {

	public static final Log LOG_PPE = LogFactory.getLog(PreProcessEngine.class);

	/** @Fields parallelPermits : 并行运行的mr数量,1为默认表示一个个执行 **/
	public int parallelPermits = 1;

	/** @Fields queues : 要基于中间结果运行的mr集合 **/
	private List<Queue> queues = new ArrayList<Queue>();

	class Queue {
		Class<Tool> clazz;
		Map<String, String> args = new HashMap<String, String>();

		public Class<Tool> getClazz() {
			return clazz;
		}

		public Map<String, String> getArgs() {
			return args;
		}

	}

	public PreProcessEngine() {
		super();
	}

	@SuppressWarnings("unchecked")
	public PreProcessEngine(XMLConfiguration config)
			throws ClassNotFoundException {
		this.parallelPermits = config.getInt("properties.parallel.permits", 1);
		List<HierarchicalConfiguration> fields = config
				.configurationsAt("properties.mapreduce(0).queues(0).queue");
		for (int x = 0; x < fields.size(); x++) {
			HierarchicalConfiguration sub = fields.get(x);
			Queue q = new Queue();
			if (StringUtils.isNotEmpty(sub.getString("class"))) {
				q.clazz = (Class<Tool>) Class.forName(sub.getString("class"));
			}
			q.args = Json.fromJson(Map.class, sub.getProperty("args")
					.toString().replace("[", "").replace("]", ""));
			this.queues.add(q);
		}
	}

	@SuppressWarnings("unchecked")
	public void execute(final String args, final String path) {
		ExecutorService executor = Executors.newFixedThreadPool(this.queues
				.size());
		LOG_PPE.info("queue size:" + queues.size());
		if (queues != null) {
			final Semaphore semaphore = new Semaphore(this.parallelPermits);
			final CountDownLatch begin = new CountDownLatch(1);
			final CountDownLatch end = new CountDownLatch(this.queues.size());
			final Map<String, String> _map = Json.fromJson(Map.class, args);
			for (Iterator<Queue> it = this.queues.iterator(); it.hasNext();) {
				final Queue hc = it.next();
				executor.submit(new Runnable() {
					public void run() {
						try {
							semaphore.acquire();
							begin.await();
							Map<String, String> map = new HashMap<String, String>();
							String jn = _map.get("jobName");
							map.put("jobName",
									hc.getArgs().get("jobName") 
									+ (StringUtils.isNotEmpty(jn) ? "["+ _map.get("jobName") + "]": ""));
							
							map.put("logTypes", hc.getArgs().get("logTypes"));
							map.put("granularity",_map.get("granularity"));
							map.put("dateTime", _map.get(Constant.DATETIME).toString());
							map.put("configFile", hc.getArgs().get("configFile"));
							if (StringUtils.isNotEmpty(path)) {
								map.put(Constant.APP_TMP_PATH, path);
							}
							map.put("mapReduceClassName",_map.get("mapReduceClassName"));
							String code = _map.get("任务标志");
							if (StringUtils.isNotEmpty(code)) {
								map.put("任务标志", code);
							}
							String json = Json.toJson(map);
							LOG_PPE.info("启动任务 [" + json + "]");
							
							ToolRunner.run(new Configuration(), hc.getClazz().newInstance(),
									new String[] { json });
							
						} catch (InterruptedException e) {
							LOG_PPE.error("", e);
							e.printStackTrace();
						} catch (Exception e) {
							LOG_PPE.error(e.getMessage());
							e.printStackTrace();
						} finally {
							semaphore.release();
							end.countDown();
						}
					}
				});
			}
			LOG_PPE.info("根据中间结果开始启动后续MR,个数为" + queues.size());
			begin.countDown();
			try {
				end.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOG_PPE.info("所有MR计算完毕!");
			executor.shutdown();
		} else {
			LOG_PPE.info("后续要基于该中间结果的MapReduce个数为空");
		}
	}
}