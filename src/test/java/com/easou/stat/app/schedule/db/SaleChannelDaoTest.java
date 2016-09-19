package com.easou.stat.app.schedule.db;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class SaleChannelDaoTest {
	private SaleChannelDao dao;
	@Before
	public void setup() {
		dao = new SaleChannelDao();
	}
	@Test
	public void testQuery() throws Exception {
		List<SaleChannelModel> list = dao.query();
		for(SaleChannelModel model : list) {
			System.out.println(model);
		}
	}

}
