package com.easou.stat.app.schedule.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.easou.stat.app.schedule.db.EventDefineModel;
import com.easou.stat.app.schedule.db.EventDefineModel.DimDefineModel;
import com.easou.stat.app.schedule.db.EventDefineModel.StatDefineModel;
import com.easou.stat.app.schedule.db.EventRuleDao;
import com.easou.stat.app.schedule.db.EventRuleDbModel;

public class EventRuleUtil {
	private static EventRuleUtil instance = new EventRuleUtil();
	private static Map<String, EventDefineModel> events = new HashMap<String, EventDefineModel>();
	private final String FILE_DIR = "event_rule.dat";
	private final String LINE_ENDING = "\r\n";
	private final String ENCODING = "UTF-8";
	private EventRuleUtil() {
		
	}
	public static EventRuleUtil getInstance() {
		return instance;
	}
	
	public EventDefineModel getEvent(String id) {
		return events.get(id);
	}
	
	/**
	 * import event rules to memory from local files.
	 */
	public void importRules(Path[] localFiles) {
		try {
//			URL urlConfig = Thread.currentThread().getContextClassLoader().getResource(FILE_DIR);
//			System.out.println(urlConfig.getPath());
			List<String> cons = new ArrayList<String>();//FileUtils.readLines(new File(urlConfig.toURI()), ENCODING);
			for(int i=0; i< localFiles.length; i++){
				String aKeyWord;
				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
				while((aKeyWord = br.readLine()) != null){
					cons.add(aKeyWord);
				}
				br.close();
			}
			
			for(String line : cons) {
				JSONObject jo = JSON.parseObject(line);
				EventDefineModel model = new EventDefineModel();
				model.setId(jo.getString("id"));
				model.setType(jo.getString("type"));
				model.setAppkey(jo.getString("appkey"));
				JSONArray jarr = jo.getJSONArray("rules");
				for(int i = 0, size = jarr.size(); i < size; i ++) {
					JSONObject dims = jarr.getJSONObject(i);
					JSONObject dim = dims.getJSONObject("dim");
					DimDefineModel dimModel = model.new DimDefineModel();
					dimModel.setId(dim.getString("id"));
					dimModel.setAll(Boolean.parseBoolean(dim.getString("isall")));
					JSONArray stats = dims.getJSONArray("sta");
					for(int j = 0, ssize = stats.size(); j < ssize; j ++) {
						JSONObject stat = stats.getJSONObject(j);
						StatDefineModel statModel = model.new StatDefineModel();
						statModel.setId(stat.getString("id"));
						statModel.setMethod(stat.getString("method"));
						dimModel.AddStat(statModel);
					}
					model.addDim(dimModel);
				}
				events.put(model.getId()+model.getAppkey(), model);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("event rules size:" + events.size());
	}
	/**
	 * export event rules to local files from db table
	 */
	public void exportRules() {
		EventRuleDao dao = new EventRuleDao();
		try {
			List<EventRuleDbModel> list = dao.query();
			List<String> out = new ArrayList<String>();
			for(EventRuleDbModel model : list) {
				JSONObject jo = new JSONObject();
				jo.put("id", model.getId());
				jo.put("type", model.getType());
				JSONArray jarr = JSON.parseArray(model.getRules());
				jo.put("rules", jarr);
				out.add(jo.toJSONString());
			}
			URL urlConfig = Thread.currentThread().getContextClassLoader().getResource(FILE_DIR);
			File outFile = new File(urlConfig.toURI());
			FileUtils.writeLines(outFile, ENCODING, out, LINE_ENDING, false);
			System.out.println("export event rules over, file:" + outFile.getAbsolutePath());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
