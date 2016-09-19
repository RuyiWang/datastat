package com.easou.stat.app.util;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nutz.json.Json;

import com.easou.stat.app.schedule.util.JobUtils;

public class MREngine {
	public static final Log LOG_MREngine = LogFactory.getLog(MREngine.class);

	/** @Fields parallelPermits : 并行运行的mr数量,1为默认表示一个个执行 **/
	public int parallelPermits = 2;

	/** @Fields queues : 要基于中间结果运行的mr集合 **/
	private List<Map<String, String>> queues = new ArrayList<Map<String, String>>();


	@SuppressWarnings("unchecked")
	public MREngine(String  fileName) throws FileNotFoundException {
		if (StringUtils.isNotEmpty(fileName) && fileName.toLowerCase().endsWith(".xml")) {
			XMLConfiguration config = loadXMLConfig(fileName);
			this.parallelPermits = config.getInt("tasks.parallel.permits", 2);
			List<HierarchicalConfiguration> fields = config
					.configurationsAt("properties.tasks(0).task");
			for (int x = 0; x < fields.size(); x++) {
				HierarchicalConfiguration sub = fields.get(x);
				Map<String ,String> map = Json.fromJson(Map.class, sub.getProperty("args")
						.toString().replace("[", "").replace("]", ""));
				Date dt = new Date();
				// 获取JOB_NAME 序号标识
				final String mrScheduleSerialNumber = JobUtils.getDateTime(dt, "yyyyMMddHHmmssSSS");
				String systemDate = JobUtils.getDateTime(dt, "yyyyMMddHHmm"); // 得到精确到秒的表示：201210012201
				
				String strDate = JobUtils.getBeforeDate2(map.get("granularity"), systemDate);
				map.put("dateTime", strDate);
				map.put("jobName", "[SN=" + mrScheduleSerialNumber + "]");
				System.out.println(map.toString());
				this.queues.add(map);
				try {
					Thread.sleep(10L);
				} catch (InterruptedException e) {
				}
			}
		}else{
			throw new FileNotFoundException(fileName);
		}
		
	}

	public void execute() {
		ExecutorService executor = Executors.newFixedThreadPool(this.queues.size());
		LOG_MREngine.info("queue size:" + queues.size());
		if (queues != null) {
			final Semaphore semaphore = new Semaphore(this.parallelPermits);
			final CountDownLatch begin = new CountDownLatch(1);
			final CountDownLatch end = new CountDownLatch(this.queues.size());
			for (Iterator<Map<String, String>> it = this.queues.iterator(); it.hasNext();) {
				final Map<String, String> map = it.next();
				executor.submit(new Runnable() {
					public void run() {
						try {
							semaphore.acquire();
							begin.await();
							String[] argsOfJob = new String[] { map.get("logTypes"), map.get("granularity"), map.get("configFile"),map.get("mapReduceClassName"), map.get("dateTime"), map.get("jobName")};
							JobUtil.submitJobByCalssName(argsOfJob);
							
						} catch (InterruptedException e) {
							LOG_MREngine.error("", e);
							e.printStackTrace();
						} catch (Exception e) {
							LOG_MREngine.error(e.getMessage());
							e.printStackTrace();
						} finally {
							semaphore.release();
							end.countDown();
						}
					}
				});
			}
			LOG_MREngine.info("Granularity TASK,个数为" + queues.size());
			begin.countDown();
			try {
				end.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOG_MREngine.info("GranularityMR计算完毕!");
			executor.shutdown();
		} else {
			LOG_MREngine.info("Granularity Task 没有配置");
		}
	}
	
	private XMLConfiguration loadXMLConfig(String fileName) {
		XMLConfiguration c = null;
		try {
			if (c == null) {
				String pathForConfig = System.getProperty("user.dir") + "/"
						+ fileName;
				c = new XMLConfiguration(pathForConfig);
				LOG_MREngine.info("加载配置[catch]" + pathForConfig);
			}
		} catch (Exception e) {
			if (c == null) {
				URL urlConfig = Thread.currentThread().getContextClassLoader()
						.getResource(fileName);
				try {
					c = new XMLConfiguration(urlConfig);
					LOG_MREngine.info("加载配置[ok]" + urlConfig);
				} catch (ConfigurationException e1) {
					LOG_MREngine.info("加载配置配置文件失败" + urlConfig, e1);
				}
			}
		}
		return c;
	}
	
}
