package com.easou.stat.app.schedule.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.nutz.dao.Sqls;
import org.nutz.dao.sql.Sql;
import org.nutz.dao.sql.SqlCallback;

import com.easou.stat.app.schedule.util.DBUtil;

public class EventRuleDao {

	public List<EventRuleDbModel> query() throws Exception {
		String sqlStr = "select t.event_id, t.event_type, t.stat_rules from sys_def_event t";
		
		Sql sql = Sqls.create(sqlStr);
		sql.setCallback(new SqlCallback() {
			
			@Override
			public Object invoke(Connection conn, ResultSet rs, Sql sql)
					throws SQLException {
				List<EventRuleDbModel> list = new ArrayList<EventRuleDbModel>();
				while (rs.next()) {
					EventRuleDbModel model = new EventRuleDbModel();
					model.setId(rs.getString(1));
					model.setType(rs.getString(2));
					model.setRules(rs.getString(3));
					list.add(model);
				}
				return list;
			}
		});
		DBUtil.dao.execute(sql);
		return sql.getList(EventRuleDbModel.class);
	}
}
