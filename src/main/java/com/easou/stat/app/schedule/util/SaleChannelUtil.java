package com.easou.stat.app.schedule.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.easou.stat.app.schedule.db.SaleChannelDao;
import com.easou.stat.app.schedule.db.SaleChannelModel;

public class SaleChannelUtil {
	private static SaleChannelUtil instance = new SaleChannelUtil();
	private List<SaleChannelModel> models = null;
	private SaleChannelUtil() {
		try {
			SaleChannelDao dao = new SaleChannelDao();
			models = dao.query();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static SaleChannelUtil getInstance() {
		return instance;
	}
	
	public String getSaleChannelByCpid(String cpid, String os) {
		//口碑用户
		if(StringUtils.isEmpty(cpid)) {
			if("andriod".equalsIgnoreCase(os))
				return "100100000000";
			if("ios".equalsIgnoreCase(os))
				return "140100000000";
			if("wp".equalsIgnoreCase(os))
				return "170100000000";
		}
		for(SaleChannelModel model : models) {
			if(isMatch(model.getRule(), cpid)) {
				return model.getFullChannel();
			}
		}
		//其他
		if("ios".equalsIgnoreCase(os))
			return "149800000000";
		if("wp".equalsIgnoreCase(os))
			return "179800000000";
		//andriod
		return "199800000000";
	}
	
	
	
	private boolean isMatch(String rule, String cpid) {
		Pattern pattern = Pattern.compile(rule);
		Matcher matcher = pattern.matcher(cpid);
		return matcher.find();
	}
}
