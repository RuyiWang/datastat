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

public class BrandModelsDao {
	private static final Log	LOG	= LogFactory.getLog(BrandModelsDao.class);
	
	
	/**
	* @Title: query
	* @Description: 查询用户自定义列表
	* @return
	* @throws Exception
	* @author vic  
	* @return List<UserDefinedEvent>
	* @throws
	*/
	public List<BrandModels> query() throws Exception {
		String sqlStr = "SELECT brand,models FROM MOBLIE_BRANDMODELS_MAP";
		Sql sql = Sqls.create(sqlStr);
		sql.setCallback(new SqlCallback() {
			@Override
			public Object invoke(Connection conn, ResultSet rs, Sql sql) throws SQLException {
				List<BrandModels> list = new LinkedList<BrandModels>();
				while (rs.next()) {
					BrandModels ude = new BrandModels();
					ude.setBrand(rs.getString("brand"));
					ude.setModels(rs.getString("models"));
					list.add(ude);
				}
				return list;
			}
		});
		DBUtil.dao.execute(sql);
		return sql.getList(BrandModels.class);
	}
}
