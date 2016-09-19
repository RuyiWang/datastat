package com.easou.stat.app.schedule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import com.easou.stat.app.schedule.util.JobUtils;

/**
 * @ClassName: StatMain
 * @Description: MR 调度主程序
 * @author sliven
 * @date 2012-5-7 上午10:41:10
 * 
 */

public class StatMain {

	private static final Log	LOG					= LogFactory.getLog(StatMain.class);
	// 调度运行不成功完成JOB列表
	private static List<String>	noFinishJobList		= new ArrayList<String>();
	// 调度所有MR运行列表
	private static List<String>	allFinishJobList	= new ArrayList<String>();

	public static void main(String[] args) {
		// 获取当前系统时间
		Date dt = new Date();
		// 获取JOB_NAME 序号标识
		final String mrScheduleSerialNumber = JobUtils.getDateTime(dt, "yyyyMMddHHmmssSSS");
		// args 参数： args[0] 是当前系统日期 args[1] 是待执行的MR列表 2 是时间粒度
		if (!(args.length == 3)) {
			if (args.length >= 5) {
				try {
					String[] args1 = new String[] { args[0], args[1], args[2], args[3], args[4],
							"[SN=" + mrScheduleSerialNumber + "]" };
					MRConfig.run(args1);
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else {
				System.out.println("args 参数：  0 是当前日期  1 是待执行的MR列表  2 是时间粒度");
				System.out.println("如: StatMain TEN_MINUTES schedule_minute.properties 201202021111");
				System.exit(2);
			}
		} else {

			try {
				String systemDate = JobUtils.getDateTime(dt, "yyyyMMddHHmm"); // 得到精确到秒的表示：20120609220101

				// 获取执行MR列表
				final List<String[]> JobsList = JobUtils.getJobsList(systemDate, args, mrScheduleSerialNumber);
				// final Map<String, List<String>> historyMap = JobUtils.getJobsMap(systemDate, args, MRScheduleSerialNumber);
				int size = JobsList.size();

				// 获取同时执行MR数量
				if (JobUtils.getMapReduceExecNums(args[0]) != 0) {
					size = JobUtils.getMapReduceExecNums(args[0]);
				}

				final int max = size;

				// 多线程 线程池
				ExecutorService JobService = Executors.newCachedThreadPool();
				final Semaphore JobSemaphore = new Semaphore(max);
				for (int jobNumber = 0; jobNumber < JobsList.size(); jobNumber++) {
					final String[] temp_list = JobsList.get(jobNumber);
					Runnable maxJob_runnable = new Runnable() {
						public void run() {
							try {
								final String[] job_list = temp_list;
								JobSemaphore.acquire();
								// MR 执行
								MRConfig.run(job_list);
								Thread.sleep((long) (5 * 1000));
							} catch (InterruptedException e) {
								e.printStackTrace();
							} finally {
								StatMain.statFinally(mrScheduleSerialNumber);
								JobSemaphore.release();
							}
						}
					};
					JobService.execute(maxJob_runnable);
				}
				JobService.shutdown();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	public static void statFinally(String mrScheduleSerialNumber) {
		try {
			Thread.sleep((long) (30 * 1000));
			Configuration conf = new Configuration();
			JobConf jobConf = new JobConf(conf);
			JobClient jobClient = new JobClient(jobConf);
			JobStatus[] jobStatus = jobClient.getAllJobs();

			for (int i = 0; i < jobStatus.length; i++) {
				// 获取本周期MR JOB信息
				RunningJob runjob = jobClient.getJob(jobStatus[i].getJobId());
				String jobId = runjob.getJobID();
				if (runjob.getJobName().indexOf("SN=" + mrScheduleSerialNumber) > -1) {

					if (runjob.isComplete()) {
						if (!runjob.isSuccessful()) {
							// 发送报警信息
							JobUtils.sendMessage(noFinishJobList, jobId, runjob.getJobName(),
									jobStatus[i].getStartTime(), runjob.getFailureInfo(), runjob.getJobState());
							noFinishJobList.add(jobId);
						}

					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
