package com.easou.stat.app.schedule.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nutz.dao.Sqls;
import org.nutz.dao.sql.Sql;
import org.nutz.dao.sql.SqlCallback;

import com.easou.stat.app.schedule.util.DBUtil;

public class UserDefinedEventDao {
	private static final Log	LOG	= LogFactory.getLog(UserDefinedEventDao.class);

	/**
	* @Title: query
	* @Description: 查询用户自定义列表
	* @return
	* @throws Exception
	* @author vic  
	* @return List<UserDefinedEvent>
	* @throws
	*/
	public List<UserDefinedEvent> query() throws Exception {
		String sqlStr = "SELECT EVENT_ID,EXPRESS FROM USER_DEFINED_EVENT";
		Sql sql = Sqls.create(sqlStr);
		sql.setCallback(new SqlCallback() {
			@Override
			public Object invoke(Connection conn, ResultSet rs, Sql sql) throws SQLException {
				List<UserDefinedEvent> list = new LinkedList<UserDefinedEvent>();
				while (rs.next()) {
					UserDefinedEvent ude = new UserDefinedEvent();
					ude.setEventId(rs.getString("EVENT_ID"));
					ude.setExpress(rs.getString("EXPRESS"));
					list.add(ude);
				}
				return list;
			}
		});
		DBUtil.dao.execute(sql);
		return sql.getList(UserDefinedEvent.class);
	}
}