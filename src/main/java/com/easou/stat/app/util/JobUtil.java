package com.easou.stat.app.util;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.schedule.util.JobUtils;

/**
 * 
 * @ClassName: JobUtil.java
 * @Description: 提交任务相关工具类
 * @author: Asa
 * @date: 2013年10月28日 上午9:58:43
 * 
 */
public class JobUtil {

	private static final Log LOG = LogFactory.getLog(JobUtil.class);

	/**
	 * 
	 * @Titile: parsePath
	 * @Description: 获取目录是否存在
	 * @author: Asa
	 * @date: 2013年10月12日 下午1:24:19
	 * @return List<String>
	 */
	public static List<String> appStatRootPath(Configuration conf, String uri,
			String dir) {
		FileSystem fileSystem;
		List<String> list = new ArrayList<String>();
		try {
			fileSystem = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(dir);
			FileStatus[] fs = fileSystem.listStatus(path);
			if (fs != null) {
				for (FileStatus f : fs) {
					String pathStr = f.getPath().toString();
					list.add(pathStr.substring(pathStr.lastIndexOf("/"),
							pathStr.length()) + "/");
				}
			}

		} catch (IOException e) {
			LOG.equals("获取目录是否存在异常");
		}
		return list;

	}

	/**
	 * 
	 * @Titile: appLogTypePath
	 * @Description: 检查日类型是否存在
	 * @author: Asa
	 * @date: 2013年10月26日 下午5:35:48
	 * @return List<String>
	 */
	public static boolean isaLogTypeExist(Configuration conf, String uri,
			String dir) {
		FileSystem fileSystem;
		boolean flag = false;
		try {
			fileSystem = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(dir);
			FileStatus[] fs = fileSystem.listStatus(path);
			if (fs == null || fs.length == 0) {
				flag = false;
			} else {
				flag = true;
			}

		} catch (IOException e) {
			flag = false;
			LOG.error("检测目录异常", e);
		}
		return flag;
	}

	/**
	 * 
	 * @Titile: submitJob
	 * @Description: 批量提交任务
	 * @author: Asa
	 * @date: 2013年10月12日 下午1:46:49
	 * @return void
	 */
	public static void submitJob(Class<?> c, String[] args) {
		// Configuration conf = new Configuration();
		// List<String> appStatRootPath = JobUtil.appStatRootPath(conf,
		// Constant.HDFSURI, Constant.LOGROOTDIR);
		// for (String appLogPath : appStatRootPath ){
		// if(JobUtil.isaLogTypeExist(conf, Constant.HDFSURI,
		// Constant.LOGROOTDIR+ appLogPath + args[0].toLowerCase())){
		// new Thread(new JobUtil.submitJobThread(className, args,
		// appLogPath),appLogPath).start();
		// }
		//
		// }
		new Thread(new JobUtil.submitJobThread(c, args), args[3]).start();
	}

	/**
	 * @Titile: submitJobByCalssName
	 * @Description:
	 * @author: Asa
	 * @date: 2014年3月14日 上午11:47:09
	 * @return void
	 */
	public static void submitJobByCalssName(String[] args) {

		new Thread(new JobUtil.submitJobByNameThread(args)).start();
	}

	/**
	 * @ClassName: CommonUtils.java
	 * @Description: 提交任务线程
	 * @author: Asa
	 * @date: 2013年10月28日 上午9:56:55
	 */
	private static class submitJobThread implements Runnable {

		private String[] runParam = new String[6];
		private Class<?> c;

		public submitJobThread(Class<?> c, String[] args) {
			this.c = c;
			runParam = args;
		}

		@Override
		public void run() {
			String[] _args = new String[] { MRDriver.argsToJson(runParam) };
			int res = 0;
			try {
				Tool tool = (Tool) Class.forName(c.getName()).newInstance();
				long s1 = System.currentTimeMillis();
				res = ToolRunner.run(new Configuration(), tool, _args);
				long s2 = System.currentTimeMillis();
				LOG.info(Thread.currentThread().getName() + " run stats[" + res
						+ "] cost time: " + ((s2 - s1) / 1000) + " s!");
			} catch (Exception e) {
				LOG.error(Thread.currentThread().getName() + "运行异常", e);
			}
		}

	}

	/**
	 * @ClassName: JobUtil.java
	 * @Description:
	 * @author: Asa
	 * @date: 2014年3月14日 上午11:51:26
	 * 
	 */
	private static class submitJobByNameThread implements Runnable {

		private String[] runParam = new String[6];

		public submitJobByNameThread(String[] args) {
			runParam = args;
		}

		@Override
		public void run() {
			String[] _args = new String[] { MRDriver.argsToJson(runParam) };
			int res = 0;
			try {
				Tool tool = (Tool) Class.forName(
						Constant.MR_BASEPACKAGE + runParam[3]).newInstance();
				long s1 = System.currentTimeMillis();
				res = ToolRunner.run(new Configuration(), tool, _args);
				long s2 = System.currentTimeMillis();
				LOG.info(Thread.currentThread().getName() + " run stats[" + res
						+ "] cost time: " + ((s2 - s1) / 1000) + " s!");
			} catch (Exception e) {
				LOG.error(Thread.currentThread().getName() + "运行异常", e);
			}
		}

	}

	public static void getJobStatus(String mrScheduleSerialNumber) {
		try {
			Configuration conf = new Configuration();
			JobConf jobConf = new JobConf(conf);
			JobClient jobClient = new JobClient(jobConf);
			JobStatus[] jobStatus = jobClient.getAllJobs();

			for (int i = 0; i < jobStatus.length; i++) {
				RunningJob runjob = jobClient.getJob(jobStatus[i].getJobId());
				String jobId = runjob.getJobID();
				if (runjob.getJobName().indexOf("SN=" + mrScheduleSerialNumber) > -1) {
					if (runjob.isComplete()) {
						if (!runjob.isSuccessful()) {
							JobUtils.sendMessage(jobId, runjob.getJobName(),
									jobStatus[i].getStartTime(),
									runjob.getFailureInfo(),
									runjob.getJobState());
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
