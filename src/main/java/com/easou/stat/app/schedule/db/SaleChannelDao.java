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

public class SaleChannelDao {

	public List<SaleChannelModel> query() throws Exception {
		String sqlStr = "select t.n_schn_id,t.n_schn_frtid,t.n_schn_secid,t.vc_schn_rule,t.n_schn_fullcid from dim_salechannel t where t.n_schn_status=1 and t.vc_schn_rule<>'*' and t.vc_schn_rule<>'^' order by t.n_schn_id desc";
		
		Sql sql = Sqls.create(sqlStr);
		sql.setCallback(new SqlCallback() {
			
			@Override
			public Object invoke(Connection conn, ResultSet rs, Sql sql)
					throws SQLException {
				List<SaleChannelModel> list = new ArrayList<SaleChannelModel>();
				while (rs.next()) {
					SaleChannelModel model = new SaleChannelModel();
					model.setId(rs.getString(1));
					model.setFirstChannel(rs.getString(2));
					model.setSecondChannel(rs.getString(3));
					model.setRule(rs.getString(4));
					model.setFullChannel(rs.getString(5));
					list.add(model);
				}
				return list;
			}
		});
		DBUtil.dao.execute(sql);
		return sql.getList(SaleChannelModel.class);
	}
}
