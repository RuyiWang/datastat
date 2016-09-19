package com.easou.stat.app;

import java.io.FileNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easou.stat.app.util.MREngine;
/**
 * 
 * @ClassName: AppMREngine.java
 * @Description: 
 * @author: Asa
 * @date: 2014年3月21日 下午2:49:42
 *
 */
public class AppMREngine {
	
	private static final Log	LOG	= LogFactory.getLog(AppMain.class);
	public static void main(String[] args){
		try {
			MREngine engine = new MREngine("app_hour_task.xml");
			engine.execute();
			
		} catch (FileNotFoundException e) {
			LOG.info("task file not fonud", e);
		}
	}
}
