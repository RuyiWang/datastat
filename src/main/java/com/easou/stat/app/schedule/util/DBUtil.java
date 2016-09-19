package com.easou.stat.app.schedule.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.nutz.dao.Dao;
import org.nutz.dao.impl.NutDao;

import com.easou.stat.app.schedule.db.Config;

/**
 * @ClassName: DBUtil
 * @Description: 数据库工具
 * @author sliven
 * @date 2012-5-29 下午3:51:30
 * 
 **/
public class DBUtil {

	public static Dao	dao	= null;

	static {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(Config.getProperty("jdbc.connection.driver"));
		ds.setUsername(Config.getProperty("jdbc.connetion.username"));
		ds.setPassword(Config.getProperty("jdbc.connetion.password"));
		ds.setUrl(Config.getProperty("jdbc.connetion.url"));
		ds.setInitialSize(Config.getIntegerProperty("jdbc.datasource.initialsize"));
		ds.setMaxActive(Config.getIntegerProperty("jdbc.datasource.maxactive"));
		ds.setMaxIdle(Config.getIntegerProperty("jdbc.datasource.maxidle"));
		ds.setMinIdle(Config.getIntegerProperty("jdbc.datasource.minidle"));
		ds.setMaxWait(Config.getIntegerProperty("jdbc.datasource.maxwait"));
		try {
			System.out.println("dao init");
			dao = new NutDao(ds);
		} catch(Exception e) {
			System.out.println("dao error");
			e.printStackTrace();
		}
	}
}
