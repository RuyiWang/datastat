package com.easou.stat.app.schedule.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.easou.stat.app.schedule.db.SaleChannelModel;

public class SaleChannelUtilTest {
	private List<SaleChannelModel> models = null;
	
	@Before
	public void setUp() {
		models = loadRules();
	}
	private List<SaleChannelModel> loadRules() {
		String path = "d:\\temp\\rules.csv";
		File file = new File(path);
		FileReader fr = null;
		BufferedReader br = null;
		List<SaleChannelModel> list = new ArrayList<SaleChannelModel>();
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null) {
				String[] strs = line.split(",");
				SaleChannelModel model = new SaleChannelModel();
				model.setId(strs[0]);
				model.setFirstChannel(strs[1]);
				model.setSecondChannel(strs[2]);
				model.setFullChannel(strs[3]);
				model.setRule(strs[4]);
				list.add(model);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
				try {
					if(br != null)
						br.close();
					if(fr != null)
						fr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return list;
	}
	private List<String> loadFile(String path) {
		File file = new File(path);
		FileReader fr = null;
		BufferedReader br = null;
		List<String> list = new ArrayList<String>();
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null) {
				list.add(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
				try {
					if(br != null)
						br.close();
					if(fr != null)
						fr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return list;
	}
	@Test
	public void testGetSaleChannelByCpid() {
		List<String> cids = loadFile("d:\\temp\\cidlist.txt");
		List<String> out = new ArrayList<String>();
		for(String cid : cids) {
			out.add(cid + "," + getSaleChannelByCpid(cid, "ios") + "\r\n");
		}
		writeFile(out, "d:\\temp\\out.csv");
	}
	private void writeFile(List<String> datas, String path) {
		File file = new File(path);
		FileWriter out = null;
		BufferedWriter writer = null;
		try {
			out = new FileWriter(file);
			writer = new BufferedWriter(out);
			String line = null;
			for(String data : datas) {
				writer.write(data);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
				try {
					if(writer != null)
						writer.close();
					if(out != null)
						out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
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
