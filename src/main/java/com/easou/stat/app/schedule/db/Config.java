package com.easou.stat.app.schedule.db;

import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * @ClassName: Config
 * @Description: 读取配置文件
 * @author 张羽强
 * @date 2012-6-11 下午2:47:49
 * 
 */
public class Config {

	private static Logger		log	= Logger.getLogger(Config.class);

	private static Properties	config;

	/**
	 * @Title: getProperty
	 * @Description: 根据key获得配置属性值
	 * @param key 配置的健
	 * @return String 值
	 * @throws
	 */
	public static String getProperty(String key) {
		String ret = null;
		try {
			if (config == null) {
				config = new Properties();
				config.load(Config.class.getResourceAsStream("/schedule_config.properties"));
			}
			ret = config.getProperty(key);
		} catch (Exception e) {
			log.error("加载配置属性: " + key + " 错误", e);
		}
		return ret;
	}

	public static Integer getIntegerProperty(String key) {
		Integer ret = null;
		try {
			if (config == null) {
				config = new Properties();
				config.load(Config.class.getResourceAsStream("/schedule_config.properties"));
			}
			if (config.getProperty(key) != null) {
				ret = Integer.valueOf(config.getProperty(key));
			}
		} catch (Exception e) {
			log.error("加载配置属性: " + key + " 错误", e);
		}
		return ret;
	}
}
