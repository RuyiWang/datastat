package com.easou.stat.app.schedule.util;

import java.util.List;

import com.easou.stat.app.schedule.db.EventDefineModel;
import com.easou.stat.app.schedule.db.UserDefinedEvent;
import com.easou.stat.app.schedule.db.UserDefinedEventDao;

public class Test {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
//		String str = "[{\"dim\":{\"id\":\"item\",\"isall\":\"true\"},\"sta\":[]},{\"dim\":{\"id\":\"cpid\",\"isall\":\"true\"},\"sta\":[]},{\"dim\":{\"id\":\"cpid,item\",\"isall\":\"true\"},\"sta\":[]}]";
//		JSONObject jo = new JSONObject();
//		jo.put("id", "eid");
//		jo.put("type", "2");
//		//JSONArray jarr = JSON.parseArray(str);
//		jo.put("rules", str);
//		System.out.println(jo.toJSONString());
		EventRuleUtil util = EventRuleUtil.getInstance();
		util.exportRules();
		EventDefineModel model = util.getEvent("id_test00");
		
//		UserDefinedEventDao userDefinedEventDao = new UserDefinedEventDao();
//		List<UserDefinedEvent> list = userDefinedEventDao.query();
//		for(UserDefinedEvent event : list) {
//			System.out.println(event);
//		}
	}

}
