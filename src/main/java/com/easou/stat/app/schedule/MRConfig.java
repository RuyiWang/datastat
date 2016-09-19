package com.easou.stat.app.schedule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easou.stat.app.util.MRDriver;
 
public class MRConfig {
	private static final Log	LOG					= LogFactory.getLog(MRConfig.class);
	public static void run(String[] args) {

		try {
			long s1 = System.currentTimeMillis();
			Integer mrIndex = Integer.valueOf(args[4]);
			String[] _args = new String[] { MRDriver.argsToJson(args) };

			switch (mrIndex) {
			/** 正式MR **/
 
				
				
			default:
				break;
			}
			
			long s2 = System.currentTimeMillis();

			LOG.info("运行状态[" + mrIndex + "] 任务运行共消耗了 " + ((s2 - s1) / 1000) + " 秒!");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
