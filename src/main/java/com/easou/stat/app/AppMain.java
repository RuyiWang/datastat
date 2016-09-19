package com.easou.stat.app;

import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.easou.stat.app.schedule.util.JobUtils;
import com.easou.stat.app.util.JobUtil;

/**
 * 
 * @Title: AppMain.java
 * @Description: App数据分析MapReduce主程序
 * @author: ASA
 * @date: 2013年10月29日 下午4:30:57
 */
public class AppMain {

	private static final Log	LOG	= LogFactory.getLog(AppMain.class);

	public static void main(String[] args) {
		// args = new String[] { "B", "HOUR","parameters_hour_client.xml", "ClassName" };
		// args = new String[] { "B", "DAY", "parameters_day_client.xml", "1" };
		// args = new String[] { "B", "WEEK","parameters_week_client.xml", "1" };
		// args = new String[] { "B", "MONTH","parameters_month_client.xml", "1" };
		if (args.length < 4 || args.length > 6) {
			System.exit(2);
		}

		// 获取当前系统时间
		Date dt = new Date();
		// 获取JOB_NAME 序号标识
		final String mrScheduleSerialNumber = JobUtils.getDateTime(dt, "yyyyMMddHHmmssSSS");
		String systemDate = JobUtils.getDateTime(dt, "yyyyMMddHHmm"); // 得到精确到秒的表示：201210012201
		
		try {
			String strDate = JobUtils.getBeforeDate2(args[1], systemDate);
			String dateStr = strDate;
			if(args.length == 5)
				dateStr = args[4];
			String[] argsOfJob = new String[] { args[0], args[1], args[2], args[3], dateStr, "[SN=" + mrScheduleSerialNumber + "]"};
			JobUtil.submitJobByCalssName(argsOfJob);
			
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}
}
