package com.easou.stat.app.schedule.util;

import java.io.IOException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.schedule.db.Config;
import com.easou.stat.app.schedule.message.SendMail;
import com.easou.stat.app.util.CommonUtils;
import com.easou.stat.app.util.DateUtil;

/**
 * @ClassName: JobUtils
 * @Description: Job工具方法类
 * @author sliven
 * @date 2012-5-7 上午10:41:10
 * 
 */
public final class JobUtils {
	private static final Log	LOG	= LogFactory.getLog(JobUtils.class);

	/**
	 * 
	 * 获取前一日期
	 * 
	 * @param granularity
	 * @param strDate
	 * @return BeforeDate
	 * @throws IOException
	 */

	public static String getBeforeDate(String granularity, String strDate) throws IOException {

		try {
			Granularity i = Granularity.valueOf(granularity);
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
			java.util.Date utilDate = format.parse(strDate);
			Calendar cal = format.getCalendar();
			cal.setTime(utilDate);

			int granularityTime = 0;

			switch (i) {
			case YEAR:
				cal.add(Calendar.YEAR, -1);
				return format.format(cal.getTime());
			case HALF_YEAR:
				cal.add(Calendar.YEAR, -6);
				return format.format(cal.getTime());
			case QUARTER:
				cal.add(Calendar.MONTH, -3);
				return format.format(cal.getTime());
			case MONTH:
				cal.add(Calendar.MONTH, -1);
				return format.format(cal.getTime());
			case NATURE_WEEK:
				cal.add(Calendar.DATE, -7);
				return format.format(cal.getTime());
			case WEEK:
				int t = Integer.valueOf(strDate.substring(6, 8));
				if (t >= 0 && t <= 28) {
					cal.add(Calendar.DATE, -7);
				} else {
					cal.add(Calendar.DATE, -10);
				}
				return format.format(cal.getTime());
			case DAY:
				cal.add(Calendar.DATE, -1);
				return format.format(cal.getTime());
			case HOUR:
				cal.add(Calendar.HOUR, -1);
				return format.format(cal.getTime());
			case TEN_MINUTES:
				if (Config.getIntegerProperty("before.granularity.ten.minutes") != null) {
					granularityTime = Config.getIntegerProperty("before.granularity.ten.minutes");
				} else {
					granularityTime = -10;
				}
				cal.add(Calendar.MINUTE, granularityTime);
				return format.format(cal.getTime());
			default:
				return "";
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "";
	}
	
	public static String getBeforeDate2(String granularity,String strDate) {

		try {
			Granularity i = Granularity.valueOf(granularity);
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
			java.util.Date utilDate = format.parse(strDate);
			Calendar cal = format.getCalendar();
			cal.setTime(utilDate);

			int granularityTime = 0;

			switch (i) {
			case YEAR:
				cal.add(Calendar.YEAR, -1);
				return format.format(cal.getTime());
			case HALF_YEAR:
				cal.add(Calendar.YEAR, -6);
				return format.format(cal.getTime());
			case QUARTER:
				cal.add(Calendar.MONTH, -3);
				return format.format(cal.getTime());
			case MONTH:
				cal.add(Calendar.MONTH, -1);
				return format.format(cal.getTime());
			case NATURE_WEEK:
				cal.add(Calendar.DATE, -7);
				return format.format(cal.getTime());
			case WEEK:
				cal.add(Calendar.DATE, -1);
				cal.setFirstDayOfWeek(2);
				cal.set(Calendar.DAY_OF_WEEK, 2);
				return format.format(cal.getTime());
			case DAY:
				cal.add(Calendar.DATE, -1);
				return format.format(cal.getTime());
			case DAY7:
				cal.add(Calendar.DATE, -1);
				return format.format(cal.getTime());
			case HOUR:
				cal.add(Calendar.HOUR, -1);
				return format.format(cal.getTime());
			case AHOUR:
				cal.add(Calendar.HOUR, -1);
				return format.format(cal.getTime());
			case TEN_MINUTES:
				if (Config.getIntegerProperty("before.granularity.ten.minutes") != null) {
					granularityTime = Config.getIntegerProperty("before.granularity.ten.minutes");
				} else {
					granularityTime = -10;
				}
				cal.add(Calendar.MINUTE, granularityTime);
				return format.format(cal.getTime());
			default:
				return "";
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "";
	}

	/**
	 * 
	 * 根据JobName获取JobID
	 * 
	 * @param jobClient
	 * @param jobStatus * @param jobName
	 * @return jobID
	 * @throws IOException
	 */

	public static JobID getJobIDByJobName(JobClient jobClient, JobStatus[] jobStatus, String jobName_str) {
		JobID jobID = null;
		try {
			for (int i = 0; i < jobStatus.length; i++) {
				RunningJob rj = jobClient.getJob(jobStatus[i].getJobID());
				if (rj.getJobName().trim().equals(jobName_str)) {
					jobID = jobStatus[i].getJobID();
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return jobID;
	}

	/**
	 * 
	 * 根据JobID获取Job状态
	 * 
	 * @param jobClient
	 * @param jobStatus
	 * @param jobID
	 * @return RUNNING = 1,SUCCEEDED = 2,FAILED = 3,PREP = 4,KILLED = 5
	 * @throws IOException
	 */

	public static String getStatusByJobID(JobClient jobClient, JobStatus[] jobStatus, JobID jobID) throws IOException {
		int status_int = 0;
		jobStatus = jobClient.getAllJobs();
		for (int i = 0; i < jobStatus.length; i++) {
			if (jobStatus[i].getJobID().getId() == jobID.getId()) {
				status_int = jobStatus[i].getRunState();
				break;
			}
		}

		String desc_str = "";
		switch (status_int) {
		case 1:
			desc_str = "RUNNING";
			break;
		case 2:
			desc_str = "SUCCEEDED";
			break;
		case 3:
			desc_str = "FAILED";
			break;
		case 4:
			desc_str = "PREP";
			break;
		case 5:
			desc_str = "KILLED";
			break;
		default:
			break;
		}
		return desc_str;
	}

	/**
	 * 
	 * 获取所有需要执行的Job
	 * 
	 * @param param
	 * @return JobsList
	 * @throws IOException
	 */
	public static List<String[]> getJobsList(String systemDate, String[] args, String mrScheduleSerialNumber) throws IOException {

		String granularity = args[0];
		String configFile = args[1];
		String strDate = "";
		if (args[2].length() == 12 && args[2].indexOf("20") > -1) {
			strDate = args[2];
		} else {
			strDate = JobUtils.getBeforeDate(granularity, systemDate);
		}

		List<String[]> JobsList = new ArrayList<String[]>();
		List<String> jobList = JobUtils.getScheduleProperties(configFile);
		for (String job : jobList) {
			List<String> paramList = CommonUtils.split(job, " ");

			if (paramList.size() == 3) {
				// jobNumber++;
				// 1 日志类型 2 时间粒度 3 日志时间 3 配置文件 4 MR序号 5 JobName
				String[] s = new String[] { paramList.get(0), granularity, strDate, paramList.get(1), paramList.get(2), "[SN=" + mrScheduleSerialNumber + "]" };
				JobsList.add(s);
			}

			if (paramList.size() == 4) {
				// jobNumber++;
				// 1 日志类型 2 时间粒度 3 日志时间 4 配置文件 5 MR序号 6 JobName 7 MR优先级
				String[] s = new String[] { paramList.get(0), granularity, strDate, paramList.get(1), paramList.get(2), "[SN=" + mrScheduleSerialNumber + "]", paramList.get(3) };
				JobsList.add(s);
			}

			if (paramList.size() == 5 && paramList.get(4).length() == 12) { // 比如：201203122201
				// jobNumber++;
				// 1 日志类型 2 时间粒度 3 日志时间 3 配置文件 4 MR序号 5 JobName
				String[] s = new String[] { paramList.get(0), granularity, paramList.get(4), paramList.get(1), paramList.get(2), "[SN=" + mrScheduleSerialNumber + "]", paramList.get(3) };
				JobsList.add(s);
			}
		}
		return JobsList;
	}

	/**
	 * 
	 * 获取所有需要执行的Job
	 * 
	 * @param param
	 * @return JobsList
	 * @throws IOException
	 */
	public static Map<String, List<String>> getJobsMap(String systemDate, String[] args, String MRScheduleSerialNumber) throws IOException {

		String granularity = args[0];
		String configFile = args[1];
		String strDate = "";
		if (args.length == 3) {
			strDate = args[2];
		} else {
			strDate = JobUtils.getBeforeDate(granularity, systemDate);
		}

		Map<String, List<String>> historyMap = new HashMap<String, List<String>>();
		List<String> jobList = JobUtils.getScheduleProperties(configFile);
		int jobNumber = 0;
		for (String job : jobList) {
			List<String> paramList = CommonUtils.split(job, " ");

			if (paramList.size() == 4) {
				jobNumber++;
				// 1 日志类型 2 时间粒度 3 日志时间 4 配置文件 5 MR序号 6 JobName 7 MR优先级

				List<String> list = new ArrayList<String>();
				list.add(paramList.get(0));
				list.add(granularity);
				list.add(strDate);
				list.add(paramList.get(1));
				list.add(paramList.get(2));
				list.add("[SN=" + MRScheduleSerialNumber + ":" + jobNumber + "]");
				list.add(paramList.get(3));
				historyMap.put("[SN=" + MRScheduleSerialNumber + ":" + jobNumber + "]", list);

			}

			if (paramList.size() == 3) {
				jobNumber++;
				// 1 日志类型 2 时间粒度 3 日志时间 3 配置文件 4 MR序号 5 JobName
				List<String> list = new ArrayList<String>();
				list.add(paramList.get(0));
				list.add(granularity);
				list.add(strDate);
				list.add(paramList.get(1));
				list.add(paramList.get(2));
				list.add("[SN=" + MRScheduleSerialNumber + ":" + jobNumber + "]");
				historyMap.put("[SN=" + MRScheduleSerialNumber + ":" + jobNumber + "]", list);
			}
		}
		return historyMap;
	}

	/**
	 * 
	 * 报警手机号码，邮件列表
	 * 
	 * @param param
	 * @return JobsList
	 * @throws IOException
	 */
	public static void sendMessage(List<String> noFinishJobList, String jobID, String jobName, long startTime, String Failmessage, int statu) throws NoSuchAlgorithmException, IOException {
		/** 报警 邮件 手机列表 **/
		List<String> UserList = JobUtils.getScheduleProperties("schedule_alarm.properties");
		for (String user : UserList) {
			if (!noFinishJobList.contains(jobID)) {
				if (user.trim().indexOf("@") > -1) {
					/** 发邮件 **/

					SimpleDateFormat sdf = new SimpleDateFormat("yyyy/dd/MM HH:mm:ss");
					String sDateTime = sdf.format(startTime); // 得到精确到秒的表示：08/31/2006 21:08:00

					StringBuffer sb = new StringBuffer();
					sb.append("<html><head></head><title>easou</title></head><body>");
					sb.append(user.substring(0, user.indexOf("@")) + ", 你好：<br><br>");
					sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MapReduce状态：" + JobUtils.getRunState(statu) + "<br>");
					sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;JobID：" + jobID + "<br>");
					sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;JobName：" + jobName + "<br>");
					sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;开始时间：" + sDateTime + "<br>");
					sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;失败信息：" + Failmessage + "<br><br>");
					sb.append("<br><br>--------------------------------------------------------------------<br>");
					sb.append("该邮件由系统发送，请不要回复！");
					sb.append("</body></html>");

					SendMail.send(user, "APP HADOOP MAPREDUCE 报警", sb.toString());
				} else {
					/** 发短信 **/
				}
			}
		}
	}

	public static void sendMessage(String jobID, String jobName, long startTime, String Failmessage, int statu) throws NoSuchAlgorithmException, IOException {
		/** 报警 邮件 手机列表 **/
		List<String> UserList = JobUtils.getScheduleProperties("schedule_alarm.properties");
		for (String user : UserList) {
			if (user.trim().indexOf("@") > -1) {
				/** 发邮件 **/

				StringBuffer sb = new StringBuffer();
				sb.append("<html><head></head><title>easou</title></head><body>");
				sb.append(user.substring(0, user.indexOf("@")) + ", 您好：<br><br>");
				sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MapReduce状态：" + JobUtils.getRunState(statu) + "<br>");
				sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;JobID：" + jobID + "<br>");
				sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;JobName：" + jobName + "<br>");
				sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;开始日期：" + startTime + "<br>");
				sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;失败信息：" + Failmessage + "<br><br>");
			//	sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;如果有特殊情况，请联系：asa_wang@staff.easou.com 13632649259<br>");
				sb.append("<br><br>--------------------------------------------------------------------<br>");
				sb.append("该邮件由系统发送，请不要回复！");
				sb.append("</body></html>");

				SendMail.send(user, "APP HADOOP MAPREDUCE 报警", sb.toString());
			} else {
				/** 发短信 **/
			}
		}
	}

	/**
	 * 
	 * 根据JobID获取Job状态
	 * 
	 * @param jobClient
	 * @param jobStatus
	 * @param jobID
	 * @return RUNNING = 1,SUCCEEDED = 2,FAILED = 3,PREP = 4,KILLED = 5
	 * @throws IOException
	 */

	public static String getRunState(int state) throws IOException {
		String stateStr = "";
		switch (state) {
		case 1:
			stateStr = "RUNNING";
			break;
		case 2:
			stateStr = "SUCCEEDED";
			break;
		case 3:
			stateStr = "FAILED";
			break;
		case 4:
			stateStr = "PREP";
			break;
		case 5:
			stateStr = "KILLED";
			break;
		default:
			break;
		}
		return stateStr;
	}

	/**
	 * 
	 * 根据ALL FAILED JOB
	 * 
	 * @param jobClient
	 * @param jobStatus
	 * @param jobID
	 * @return RUNNING = 1,SUCCEEDED = 2,FAILED = 3,PREP = 4,KILLED = 5
	 * @throws IOException
	 */

	public static List<String> getFailedJobs(JobClient jobClient, JobStatus[] jobStatus) throws IOException {
		List<String> failedjobList = new ArrayList<String>();
		int status_int = 0;
		jobStatus = jobClient.getAllJobs();
		for (int i = 0; i < jobStatus.length; i++) {
			String jobid = jobStatus[i].getFailureInfo();
			if (status_int == 3) {
				failedjobList.add(jobid);
			}
		}
		return failedjobList;
	}

	/**
	 * 
	 * 获取正在运行的JobID的列表
	 * 
	 * @param jobClient
	 * @return ArrayList<JobID>
	 */

	public static ArrayList<JobID> getRunningJobList(JobClient jobClient) {
		ArrayList<JobID> runningJob_list = new ArrayList<JobID>();
		JobStatus[] js;
		try {
			js = jobClient.getAllJobs();
			for (int i = 0; i < js.length; i++) {
				if (js[i].getRunState() == JobStatus.RUNNING || js[i].getRunState() == JobStatus.PREP) {

					runningJob_list.add(js[i].getJobID());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return runningJob_list;
	}

	public static String getDateTime(Date d, String dFormat) {
		SimpleDateFormat SNsdf = new SimpleDateFormat(dFormat);
		String SNsystemDate = SNsdf.format(d); // 得到精确到秒的表示：08/31/2006 21:08:00
		return SNsystemDate;
	}

	public static int getMapReduceExecNums(String granularity) {
		if (Config.getIntegerProperty("mapreduce_" + granularity.toLowerCase() + "_execnums") != null)
			return Config.getIntegerProperty("mapreduce_" + granularity.toLowerCase() + "_execnums");
		else
			return 0;
	}

	@SuppressWarnings("rawtypes")
	private static List<String> getScheduleProperties(String fileName) {

		PropertiesConfiguration c = null;
		try {
			if (c == null) {
				String pathForConfig = System.getProperty("user.dir") + "/" + fileName;
				c = new PropertiesConfiguration(pathForConfig);
			}
		} catch (Exception e) {
			if (c == null) {
				URL urlConfig = Thread.currentThread().getContextClassLoader().getResource(fileName);
				try {
					c = new PropertiesConfiguration(urlConfig);
				} catch (ConfigurationException e1) {
					e1.printStackTrace();
				}
			}
		}

		List<String> list = new ArrayList<String>();
		for (Iterator it = c.getKeys(); it.hasNext();) {
			String name = it.next().toString();
			for (Object s : c.getList(name)) {
				list.add(name + " " + s.toString());
				LOG.info(name + " " + s.toString());
			}
		}
		return list;
	}

	public static Map<String, String> getCounterList(String counters) {
		Map<String, String> map = new HashMap<String, String>();
		StringTokenizer itr = new StringTokenizer(counters, "\n");
		while (itr.hasMoreTokens()) {
			String parKey = itr.nextToken();
			if (parKey.indexOf("Data-local map tasks=") > -1) {
				map.put("mapTasks", parKey.replace("Data-local map tasks=", "").replace("		", ""));
			}
			if (parKey.indexOf("		Map output records=") > -1) {
				map.put("mapRecords", parKey.replace("Map output records=", "").replace("		", ""));
			}
			if (parKey.indexOf("Launched reduce tasks=") > -1) {
				map.put("reduceTasks", parKey.replace("Launched reduce tasks=", "").replace("		", ""));
			}
			if (parKey.indexOf("Reduce output records") > -1) {
				map.put("reduceRecords", parKey.replace("Reduce output records=", "").replace("		", ""));
			}
		}
		return map;
	}
	
	public static void main(String[] args){
		System.out.println(JobUtils.getBeforeDate2("MONTH", "201312011010"));
	}
}
