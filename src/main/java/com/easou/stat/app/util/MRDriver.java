package com.easou.stat.app.util;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.nutz.json.Json;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Dimensions;
import com.easou.stat.app.constant.Granularity;
import com.easou.stat.app.constant.Indicators;
import com.easou.stat.app.constant.LogTypes;
import com.easou.stat.app.schedule.db.BrandModels;
import com.easou.stat.app.schedule.db.BrandModelsDao;

public class MRDriver {

	private static Log LOG = LogFactory.getLog(MRDriver.class);
	private Set<String> paths = new HashSet<String>();
	private Granularity granularity;
	private Calendar calendar;
	private Job job;
	private Configuration conf;
	private PreProcessEngine preProcessEngine;
	private FileSystem fileSystem;
	private List<LogTypes> logTypes = new ArrayList<LogTypes>();
	private String dateTime;

	public MRDriver() {
	}

	/**
	 * Description: </p>
	 * 
	 * @param conf
	 * @param map
	 * @param bool
	 * @throws Exception
	 */
	public MRDriver(Configuration conf, String json) throws Exception {
		this(conf, json, true);
	}
	
	public MRDriver(Configuration conf, String json, boolean hasMobile) throws Exception {
		Map<String, String> map = Json.fromJson(Map.class, json);
		String logs = map.get(Constant.LOGTYPES);
		String granularity = map.get(Constant.GRANULARITY);
		String dateTime = map.get(Constant.DATETIME);
		String fileName = map.get("configFile");

		if (conf == null) {
			throw new IllegalArgumentException("参数错误");
		}
		if (StringUtils.isEmpty(logs)) {
			throw new IllegalArgumentException("没有设定统计的日志类型，或者获得统计日志类型值失败！");
		}
		if (StringUtils.isEmpty(dateTime)) {
			throw new IllegalArgumentException("没有设定统计的时间，或者获得统计时间值失败！");
		}
		if (StringUtils.isEmpty(granularity)) {
			throw new IllegalArgumentException("没有设定统计的粒度，或者获得统计时间粒度值失败！");
		}
		this.conf = conf;
		this.fileSystem = FileSystem.get(conf);
		this.calendar = CommonUtils.getCalendarByStringOfDateTime(dateTime);
		this.granularity = Granularity.valueOf(granularity.toUpperCase());
		this.logTypes = analyzeLogTypes(logs);
		this.dateTime = dateTime;

		if (StringUtils.isNotEmpty(fileName)
				&& fileName.toLowerCase().endsWith(".xml")) {
			LOG.info("加载配置文件 ");
			XMLConfiguration config = loadXMLConfig(fileName);
			LOG.info("加载校验维度类型 ");
			analyzeDimTypesWithXML(config);//分析维度类型
			LOG.info("加载配置项到环境 ");
			setConfigurationWithXML(config);
			LOG.info("解析配置文件中的指标 ");
			analyzeIndicatorsWithXML(config);

			String fmr = map.get(Constant.APP_TMP_MAPREDUCE);
			if (StringUtils.isNotEmpty(fmr) && Boolean.valueOf(fmr)) {
				LOG.info("初始化预处理引擎");
				this.preProcessEngine = new PreProcessEngine(config);
			}
		}

		LOG.info("解析计算时间 ");
		analyzeDateTime(dateTime, this.granularity);

		conf.set("logTypes", logs);
		LOG.info("日志类型有: " + logs);
		if ("B".equals(logs)) {
			if (StringUtils.isNotBlank(map.get(Constant.USER_DEFINED_EVENT_LIST))) {
				this.conf.set(Constant.USER_DEFINED_EVENT_LIST, map.get(Constant.USER_DEFINED_EVENT_LIST));
				LOG.info("用户自定义事件列表(User Defined Event): " + map.get(Constant.USER_DEFINED_EVENT_LIST));
			}
			if (StringUtils.isNotBlank(map.get(Constant.PHONE_BRAND))) {
				this.conf.set(Constant.PHONE_BRAND, getBrandModels());
			}
		}
		this.job = new Job(conf);
		this.job.setJarByClass(MRDriver.class);

		LOG.info("解析日志路径");
		analyzeLogPath(map, hasMobile);

		// 把自定义事件列表从map里去掉，简化Job Name显示
		map.remove(Constant.USER_DEFINED_EVENT_LIST);
		LOG.info("解析jobname ");
		analyzeJobName(map);
	}

	private void analyzeLogPath(Map<String, String> args, boolean hasMobile) throws IOException {
		String rootLogDir = conf.get("root.log.dir");
		if (rootLogDir == null) {
			LOG.info("没有配置HDFS日志根路径 root.log.dir,默认等于HDFS的根目录 /applog/log !");
			rootLogDir = Constant.LOGROOTDIR;
		}
		List<String> appStatRootPath = JobUtil.appStatRootPath(conf, Constant.HDFSURI, rootLogDir);
		if (StringUtils.isEmpty(args.get(Constant.APP_TMP_PATH))) {
			analyzeHDFSLogPath(appStatRootPath);
		} else {
			this.paths.clear();
			this.paths.add(args.get(Constant.APP_TMP_PATH));
		}
		if(hasMobile) {
			// 用户信息日志路径
			for(String appLogPath : appStatRootPath){
				if("/cache/".equals(appLogPath) || "/invalid/".equals(appLogPath))
					continue;
				String mRootLogDir = rootLogDir + appLogPath + "m";
				if (StringUtils.isNotEmpty(args.get(Constant.APP_MOBILEINFO_MAPREDUCE))
						&& new Path(mRootLogDir).isAbsolute()) {
					if(JobUtil.isaLogTypeExist(conf,Constant.HDFSURI,mRootLogDir)){
						//this.paths.add(mRootLogDir);
						//System.out.println("mRootLogDir:" + mRootLogDir);
						Path workDir = fileSystem.getWorkingDirectory();
						excludeEmptyFolderPaths(workDir, mRootLogDir);
					}
				
				}
			}
			
		}
	}

	/**
	 * @Title: argsToJson
	 * @Description:将运行是的参数解析为json字符
	 * @param args
	 * @return
	 * @author 廖瀚卿
	 * @return String
	 * @throws
	 */
	public static String argsToJson(String[] args) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("logTypes", args[0]);
		map.put("granularity", args[1]);
		map.put("configFile", args[2]);
		map.put("dateTime", args[4]);
		if (args.length >= 5) {
			map.put("mapReduceClassName", args[3]);
		}
		if (args.length >= 6) {
			map.put("jobName", args[5]);
		}
		return Json.toJson(map);
	}

	/**
	 * @Title: analyzeJobName
	 * @Description: 解析jobname
	 * @param args
	 * @author 廖瀚卿
	 * @return void
	 * @throws
	 */
	private void analyzeJobName(Map<String, String> args) {
		String jb = args.get("jobName");
		System.out.println("#1:"+job.getJobName());
		args.put("jobName", (StringUtils.isEmpty(jb) ? "" : jb) + "["
				+ this.job.getJobName() + "]");
		this.job.setJobName(Json.toJson(args));
		System.out.println("#2:"+job.getJobName());
	}

	/**
	 * @Title: analyzeDateTime
	 * @Description: 解析计算时间
	 * @param dateTime
	 * @param g
	 * @return
	 * @author 廖瀚卿
	 * @return String
	 * @throws
	 */
	private void analyzeDateTime(String dateTime, Granularity g) {
		this.conf.set(Constant._DATETIME, dateTime);
		switch (g) {
		case DAY:
		case DAY7:
		case WEEK:
			dateTime = dateTime.substring(0, 8);
			break;
		case HOUR:
		case AHOUR:
			dateTime = dateTime.substring(0, 10);
			break;
		case MONTH:
			dateTime = dateTime.substring(0, 6);
			break;
		}
		this.conf.set("dateTime", dateTime);
		// 设置统计时间周期
		this.conf.set(Constant.GRANULARITY, g.toString());
		LOG.info("计算的时间: " + dateTime);
	}

	/**
	 * @Title: loadIndicators
	 * @Description: 加载运算指标
	 * @param conf
	 * @author 廖瀚卿
	 * @return List<SummaryIndicators>
	 * @throws
	 */
	public static List<Indicators> loadIndicators(Configuration conf) {
		String str = conf.get("indicators");
		if (StringUtils.isEmpty(str)) {
			System.err.println("没有设定统计的指标，或者获得统计指标值失败！");
			return null;
		}

		List<String> arr = CommonUtils.split(str, ",");
		List<Indicators> indicators = new ArrayList<Indicators>();
		for (String s : arr) {
			if (StringUtils.isNotBlank(s)) {
				indicators.add(Indicators.valueOf(s)); // 初始化指标
			}
		}
		return indicators;
	}

	/**
	 * @Title: loadDimTypes
	 * @Description: 加载维度类型
	 * @param conf
	 * @author vic
	 * @return List<Dimensions>
	 * @throws
	 */
	public static List<Dimensions> loadDimTypes(Configuration conf) {
		String str = conf.get("dimtypes");
		if (StringUtils.isEmpty(str)) {
			System.err.println("没有设定统计的维度类型，或者获得维度类型值失败！");
			return null;
		}
		List<String> arr = CommonUtils.split(str, ",");
		List<Dimensions> dimensions = new ArrayList<Dimensions>();
		for (String s : arr) {
			if (StringUtils.isNotBlank(s)) {
				dimensions.add(Dimensions.valueOf(s)); // 初始化维度类型
			}
		}
		return dimensions;
	}

	public static List<LogTypes> analyzeLogTypes(String logs) {
		List<String> list = CommonUtils.split(logs, ",");
		List<LogTypes> logList = new ArrayList<LogTypes>();
		for (String s : list) {
			logList.add(LogTypes.valueOf(s.trim().toUpperCase()));
		}
		return logList;
	}

	/**
	 * @Title: loadPropertiesConfigurationList
	 * @Description: 加载xml配置文件中的维度配置信息
	 * @param config
	 * @author 廖瀚卿
	 * @return List<PropertiesConfiguration>
	 * @throws
	 */
	public static List<PropertiesConfiguration> loadPropertiesConfigurationList(
			Configuration config) {
		List<PropertiesConfiguration> plist = new ArrayList<PropertiesConfiguration>();
		List<String> dimensions = CommonUtils.split(config.get("dimensions"),
				"|");
		for (Iterator<String> it = dimensions.iterator(); it.hasNext();) {
			String d = it.next();
			List<String> dims = CommonUtils.split(d, ",");
			PropertiesConfiguration pc = new PropertiesConfiguration();
			for (Iterator<String> dit = dims.iterator(); dit.hasNext();) {
				String[] s = dit.next().split("=");
				if (s.length == 2) {
					pc.addProperty(s[0], s[1]);
				}
			}
			plist.add(pc);
		}
		return plist;
	}

	/**
	 * @throws IOException
	 * @Title: analyzeHDFSLogPath
	 * @Description: 分析HDFS日志路径
	 * @author 廖瀚卿
	 * @return void
	 * @throws
	 */
	private void analyzeHDFSLogPath(List<String> appStatRootPath) throws IOException {
		String rootLogDir = conf.get("root.log.dir");

		if (rootLogDir == null) {
			LOG.info("没有配置HDFS日志根路径 root.log.dir,默认等于HDFS的根目录 /applog/log");
			rootLogDir = "/applog/log";
		}
		Set<String> set = new HashSet<String>();

		// 获得计算日期到个单位
		String year = String.format("%04d", calendar.get(Calendar.YEAR));
		String month = String.format("%02d", calendar.get(Calendar.MONTH) + 1);
		String day = String.format("%02d", calendar.get(Calendar.DATE));
		String hour = String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY));
		String minute = String.format("%02d", calendar.get(Calendar.MINUTE));

		String s = "/";
		for(String appLogPath : appStatRootPath){
			String appRootDir = rootLogDir + appLogPath;
			
			//去除缓存文件
			if(appRootDir.contains("cache"))continue;
			
			for (LogTypes log : logTypes) {
				
				if("m".equals(log.toString().toLowerCase()))
					break;
				if("tm".equals(log.toString().toLowerCase())){
					String prefix = "/applog/tmp/" + log.toString().toLowerCase();
					switch (this.granularity) {
					case DAY:
						set.addAll(this.getTMPathsOfDay7(prefix+"/"+Granularity.DAY.toString().toLowerCase(),year+month+day+hour+minute));
						break;
					}
				}else{
					String prefix = appRootDir + log.toString().toLowerCase();
					StringBuilder path = new StringBuilder();

					switch (this.granularity) {
					case YEAR:
						set.add(path.append(prefix).append(s).append("year=")
								.append(year).append(s).toString());
						break;
					case HALF_YEAR:
						break;
					case QUARTER:
						set.addAll(getPathsOfQuarter(this.calendar, prefix));
						break;
					case MONTH:
						path.append(prefix).append(s).append("year=").append(year)
								.append(s).append("month=").append(month).append(s);
						set.add(path.toString());
						break;
					case NATURE_WEEK:
						set.addAll(getPathsOfMonthWeek(this.calendar, prefix));
						break;
					case WEEK:
						set.addAll(getPathsOfWeek(this.calendar, prefix));
						break;
					case DAY:
						if(logTypes.contains("tm")){
							SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
							try {
								Date d = sdf.parse(year+month+day);
								Calendar c = Calendar.getInstance();
								c.setTime(d);
								c.add(Calendar.DAY_OF_YEAR, -1);
								String yearTmp = String.format("%04d",  c.get(Calendar.YEAR));
								String monthTmp = String.format("%02d", c.get(Calendar.MONTH) + 1);
								String dayTmp = String.format("%02d", c.get(Calendar.DATE));
								path.append(prefix).append(s).append("year=").append(yearTmp)
								.append(s).append("month=").append(monthTmp).append(s)
								.append("day=").append(dayTmp).append(s);
								set.add(path.toString());
							} catch (ParseException e) {}
						}else{
							path.append(prefix).append(s).append("year=").append(year)
							.append(s).append("month=").append(month).append(s)
							.append("day=").append(day).append(s);
							set.add(path.toString());
						}
						
						break;
					case DAY7:
						set.addAll(this.getPathsOfDay7(prefix,year+month+day+hour+minute));
						break;
					case HOUR:
						path.append(prefix).append(s).append("year=").append(year)
								.append(s).append("month=").append(month).append(s)
								.append("day=").append(day).append(s).append("hour=")
								.append(hour).append(s);
						set.add(path.toString());
						break;
					case AHOUR:
						path.append(prefix).append(s).append("year=").append(year)
						.append(s).append("month=").append(month).append(s)
						.append("day=").append(day).append(s);
						set.addAll(this.getAllHourDayPath(path.toString()));
						break;
					}
					
				}
				
			}
		}
		excludePaths(set);

		if (paths.size() == 0) {
			LOG.info("路径为空!");
		}
	}

	/**
	 * @Title: analyzeIndicatorsWithXML
	 * @Description: 解析配置文件中的指标内容
	 * @param paramConfig
	 * @return
	 * @author 廖瀚卿
	 * @return String
	 * @throws
	 */
	@SuppressWarnings("unchecked")
	private void analyzeIndicatorsWithXML(XMLConfiguration paramConfig) {
		List<String> names = Indicators.NUMBER_USERS.getNameList();
		StringBuilder sb = new StringBuilder();

		List<HierarchicalConfiguration> fields = paramConfig
				.configurationsAt("properties.indicators(0).indicator");
		for (int i = 0; i < fields.size(); i++) {
			HierarchicalConfiguration sub = fields.get(i);
			String fieldName = sub.getString("name");
			if (names.contains(fieldName)) {
				Boolean fieldType = sub.getBoolean("value");
				if (fieldType) {
					sb.append(fieldName);
					sb.append(",");
				}
			}
		}
		String indicators = "";
		if (sb.length() > 0) {
			indicators = sb.substring(0, sb.length() - 1);
			conf.set("indicators", indicators);
			LOG.info("计算的指标: " + indicators);
		}
	}

	/**
	 * @Title: analyzeDimTypesWithXML
	 * @Description: 解析配置文件中的维度类型
	 * @param paramConfig
	 * @return
	 * @author vic
	 * @return String
	 * @throws
	 */
	@SuppressWarnings("unchecked")
	private void analyzeDimTypesWithXML(XMLConfiguration paramConfig) {
		List<String> names = Dimensions.city.getNameList();//获取所有的维度类型
		StringBuilder sb = new StringBuilder();

		List<HierarchicalConfiguration> fields = paramConfig
				.configurationsAt("properties.dimtypes(0).dimtype");
		System.out.println("fields.size():"+fields.size());
		for (int i = 0; i < fields.size(); i++) {
			HierarchicalConfiguration sub = fields.get(i);
			System.out.println("sub:"+sub);
			String fieldName = sub.getString("name");
			if (names.contains(fieldName)) {
				Boolean fieldType = sub.getBoolean("value");
				if (fieldType) {
					sb.append(fieldName);
					sb.append(",");
				}
			}
		}
		String dimtypes = "";
		if (sb.length() > 0) {
			dimtypes = sb.substring(0, sb.length() - 1);
			conf.set("dimtypes", dimtypes);
			LOG.info("计算的维度类型有: " + dimtypes);
		}
	}

	/**
	 * @Title: excludePaths
	 * @Description: 1.排除空文件夹目录,2.排除配置文件中设定的需要过滤的目录
	 * @param set
	 * @author 廖瀚卿
	 * @return void
	 * @throws
	 */
	private void excludePaths(Set<String> set) {
		paths.clear();

		Path workDir = fileSystem.getWorkingDirectory();
		for (Iterator<String> it = set.iterator(); it.hasNext();) {
			excludeEmptyFolderPaths(workDir, it.next());
		}
		System.out.println("excludePaths:paths.size():"+paths.size());
		for (Iterator<String> it = paths.iterator(); it.hasNext();) {
			String path = it.next();
			System.out.println("excludePaths:"+path);
		}

		String excludeInputs = this.conf.get("exclude.inputs");
		boolean onlyIncludeSpider = conf.getBoolean(
				"only.include.spider.inputs", false);

		if (onlyIncludeSpider) {
			for (Iterator<String> it = paths.iterator(); it.hasNext();) {
				String path = it.next();
				if (!path.substring(0, path.length() - 1).endsWith("spider")) {
					it.remove();
				}
			}
		} else if (StringUtils.isNotEmpty(excludeInputs)) {
			List<String> list = CommonUtils.split(excludeInputs, "|");
			for (String input : list) {
				if (StringUtils.isNotEmpty(input)) {
					for (Iterator<String> it = paths.iterator(); it.hasNext();) {
						String path = it.next();

						if (path.substring(0, path.length() - 1)
								.endsWith(input)) {
							it.remove();
						}
					}
				}
			}
		}
	}

	/**
	 * @Title: excludeEmptyFolderPaths
	 * @Description: 返回有文件的目录<br>
	 * @param fs
	 * @param path
	 * @param s
	 * @author 廖瀚卿
	 * @return Set<String>
	 */
	private void excludeEmptyFolderPaths(Path workDir, String path) {
		System.out.println("excludeEmptyFolderPaths:"+workDir+"  "+path);
		Path dst;
		if (null == path || "".equals(path)) {
			dst = new Path(workDir + "/" + path);
		} else {
			dst = new Path(path);
		}
		try {
			String rp = "";
			FileStatus[] list = fileSystem.listStatus(dst);
			if (list != null) {
				for (FileStatus f : list) {

					if (null != f) {
						rp = new StringBuffer().append(f.getPath().getParent())
								.append("/").append(f.getPath().getName())
								.toString();
						if (f.isDir()) { // 判断是否为文件夹，如果是文件夹，从新循环获取文件夹下的文件
							excludeEmptyFolderPaths(workDir, rp);
						} else {
							// 匹配第三列模式
							Pattern p = Pattern
									.compile("(\\w+):\\/\\/([^/]+)(/.*/)([^/]+)");
							Matcher m = p.matcher(rp);
							
							// 找到匹配路径，插入排重数组
							while (m.find()) {
								paths.add(m.group(3));
							}
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void execute(String args, String path) {
		if (preProcessEngine != null) {
			System.out.println("======================="+args);
			preProcessEngine.execute(args, path);
		}
	}

	private Boolean getConfig(
			org.apache.commons.configuration.Configuration pc, String key) {
		if (pc.containsKey(key)) {
			return pc.getBoolean(key);
		}
		return false;
	}

	public Granularity getGranularity() {
		return granularity;
	}

	public Job getJob() {
		return job;
	}

	public Path[] getPaths() {
		List<Path> ps = new ArrayList<Path>();
		for (Iterator<String> it = paths.iterator(); it.hasNext();) {
			ps.add(new Path(it.next()));
		}
		return ps.toArray(new Path[] {});
	}

	/**
	 * @Title: getPathsOfMonthWeek
	 * @Description: 
	 *               获得月周的路径，一个月分为4个周，1到7日，8到14，15到21，22到最后一天。获得当前计算到日期属于那个周就返回这周到每天到路径数据
	 * @param c
	 * @param prefix
	 * @author 廖瀚卿
	 * @return Set<String>
	 * @throws
	 */
	private Set<String> getPathsOfMonthWeek(Calendar c, String prefix) {
		SimpleDateFormat sdf = new SimpleDateFormat("/yyyy/MM/dd/");
		Set<String> set = new HashSet<String>();

		int date = c.get(Calendar.DATE);
		if (date >= 1 && date <= 7) {
			for (int i = 0; i < 7; i++) {
				c.set(Calendar.DAY_OF_MONTH, c.getMinimum(Calendar.DATE) + i);
				set.add(prefix + sdf.format(c.getTime()));

			}
		}
		if (date >= 8 && date <= 14) {
			for (int i = 7; i < 14; i++) {
				c.set(Calendar.DAY_OF_MONTH, c.getMinimum(Calendar.DATE) + i);
				set.add(prefix + sdf.format(c.getTime()));

			}
		}
		if (date >= 15 && date <= 21) {
			for (int i = 14; i < 21; i++) {
				c.set(Calendar.DAY_OF_MONTH, c.getMinimum(Calendar.DATE) + i);
				set.add(prefix + sdf.format(c.getTime()));

			}
		}
		if (date >= 22 && date <= 31) {
			for (int i = 21; i < c.getActualMaximum(Calendar.DAY_OF_MONTH); i++) {
				c.set(Calendar.DAY_OF_MONTH, c.getMinimum(Calendar.DATE) + i);
				set.add(prefix + sdf.format(c.getTime()));

			}
		}
		return set;
	}

	/**
	 * @Title: getPathsOfQuarter
	 * @Description: 获得季度路径数据
	 * @param c
	 * @param prefix
	 * @author 廖瀚卿
	 * @return Set<String>
	 * @throws
	 */
	private Set<String> getPathsOfQuarter(Calendar c, String prefix) {
		Set<String> set = new HashSet<String>();
		SimpleDateFormat sdf = new SimpleDateFormat("/yyyy/MM", Locale.CHINA);

		int month = c.get(Calendar.MONTH) + 1;

		if ((1 <= month) && (month < 4)) {
			for (int i = 0; i < 3; i++) {
				c.set(Calendar.MONTH, c.getMinimum(Calendar.MONTH) + i);
				set.add(prefix + sdf.format(c.getTime()));
			}
		}

		if ((4 <= month) && (month < 7)) {
			for (int i = 3; i < 6; i++) {
				c.set(Calendar.MONTH, c.getMinimum(Calendar.MONTH) + i);
				set.add(prefix + sdf.format(c.getTime()));
			}
		}

		if ((7 <= month) && (month < 10)) {
			for (int i = 6; i < 9; i++) {
				c.set(Calendar.MONTH, c.getMinimum(Calendar.MONTH) + i);
				set.add(prefix + sdf.format(c.getTime()));
			}
		}

		if ((10 <= month) && (month < 13)) {
			for (int i = 9; i < 12; i++) {
				c.set(Calendar.MONTH, c.getMinimum(Calendar.MONTH) + i);
				set.add(prefix + sdf.format(c.getTime()));
			}
		}
		return set;
	}

	/**
	 * @Title: getPathsOfWeek
	 * @Description: 获得自然周，周一到周日的每一天到日期
	 * @return
	 * @author 廖瀚卿
	 * @return Set<String>
	 * @throws
	 */
	private Set<String> getPathsOfWeek(Calendar c, String prefix) {
		SimpleDateFormat sdf = new SimpleDateFormat("/yyyy/MM/dd/");
		c.setFirstDayOfWeek(Calendar.MONDAY);

		Set<String> set = new HashSet<String>();
		for (int i = Calendar.SUNDAY; i <= Calendar.SATURDAY; i++) {
			c.set(Calendar.DAY_OF_WEEK, i);
			String[] time = sdf.format(c.getTime()).split("/");
			// set.add(prefix + sdf.format(c.getTime()));
			set.add(prefix + "/year=" + time[1] + "/month=" + time[2] + "/day="
					+ time[3]);
		}
		return set;
	}
	/**
	 * 
	 * @Titile: getPathsOfDay7
	 * @Description: 
	 * @author: Asa
	 * @date: 2014年3月7日 上午11:06:54
	 * @return Set<String>
	 */
	private Set<String> getPathsOfDay7(String prefix,String date_time) {
		Set<String> set = new HashSet<String>();
		try {
			Calendar calendar = CommonUtils.getCalendarByStringOfDateTime(date_time);
			SimpleDateFormat sdf = new SimpleDateFormat("/yyyy/MM/dd/");
			for (int i = 0; i <= 7; i++) {
				if(i != 0){
					calendar.add(Calendar.DAY_OF_YEAR, -1);
				}
				String[] time = sdf.format(calendar.getTime()).split("/");
				set.add(prefix + "/year=" + time[1] + "/month=" + time[2] + "/day="
						+ time[3]);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return set;
	}
	
	
	private Set<String> getTMPathsOfDay7(String prefix,String date_time) {
		Set<String> set = new HashSet<String>();
		try {
			Calendar calendar = CommonUtils.getCalendarByStringOfDateTime(date_time);
			SimpleDateFormat sdf = new SimpleDateFormat("/yyyyMMdd/");
			for (int i = 0; i <= 7; i++) {
				if(i != 0){
					calendar.add(Calendar.DAY_OF_YEAR, -1);
				}
				String time = sdf.format(calendar.getTime());
				set.add(prefix + time);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return set;
	}

	public PreProcessEngine getPreProcessEngine() {
		return preProcessEngine;
	}

	public PropertiesConfiguration loadConfig(String fileName) {
		PropertiesConfiguration c = null;
		try {
			if (c == null) {
				String pathForConfig = System.getProperty("user.dir") + "/"
						+ fileName;
				c = new PropertiesConfiguration(pathForConfig);
				LOG.info("加载配置[catch]" + pathForConfig);
			}
		} catch (Exception e) {
			if (c == null) {
				URL urlConfig = Thread.currentThread().getContextClassLoader()
						.getResource(fileName);
				try {
					c = new PropertiesConfiguration(urlConfig);
					LOG.info("加载配置[ok]" + urlConfig);
				} catch (ConfigurationException e1) {
					LOG.info("加载配置配置文件失败" + urlConfig, e1);
				}
			}
		}

		return c;
	}

	public XMLConfiguration loadXMLConfig(String fileName) {
		XMLConfiguration c = null;
		try {
			if (c == null) {
				String pathForConfig = System.getProperty("user.dir") + "/"
						+ fileName;
				c = new XMLConfiguration(pathForConfig);
				LOG.info("加载配置[catch]" + pathForConfig);
			}
		} catch (Exception e) {
			if (c == null) {
				URL urlConfig = Thread.currentThread().getContextClassLoader()
						.getResource(fileName);
				try {
					c = new XMLConfiguration(urlConfig);
					LOG.info("加载配置[ok]" + urlConfig);
				} catch (ConfigurationException e1) {
					LOG.info("加载配置配置文件失败" + urlConfig, e1);
				}
			}
		}
		return c;
	}

	/**
	 * @Title: setConfigurationWithXML
	 * @Description: 加载配置文件中的配置项到环境
	 * @param config
	 * @author 廖瀚卿
	 * @return void
	 * @throws
	 */
	private void setConfigurationWithXML(XMLConfiguration config) {
		@SuppressWarnings("unchecked")
		List<HierarchicalConfiguration> fields = config
				.configurationsAt("properties.property");
		for (Iterator<HierarchicalConfiguration> it = fields.iterator(); it
				.hasNext();) {
			HierarchicalConfiguration sub = it.next();
			String fieldName = sub.getString("name");
			String value = sub.getString("value");
			if ("tmpjars".equals(fieldName)) {
				this.conf.set(fieldName, value.replaceAll(";", ","));
				continue;
			}
			this.conf.set(fieldName, value);
		}
	}

	/**
	 * @Title: getBrandModels
	 * @Description: 获取手机品牌
	 * @return
	 * @author
	 * @return String
	 * @throws
	 */
	private String getBrandModels() {
		Map<String, String> map = new HashMap<String, String>();
		// 手机机型转版本
		BrandModelsDao dao = new BrandModelsDao();
		List<BrandModels> list = null;
		try {
			list = dao.query();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		if (null != list && list.size() > 0) {
			for (int i = 0; i < list.size(); i++) {
				BrandModels brandModels = list.get(i);
				map.put(brandModels.getModels(), brandModels.getBrand());
			}
		}
		return Json.toJson(map);
	}

	
	private Set<String> getAllHourDayPath(String pathPrefix){
		Set<String> set = new HashSet<String>();
		int hour = Integer.parseInt(this.dateTime.substring(8, 10));
		for(int i=0; i <= hour; i++){
			StringBuilder path = new StringBuilder(pathPrefix);
			if(i<10){
				path.append("hour=").append("0"+i).append("/");
			}else{
				path.append("hour=").append(i).append("/");
			}
			set.add(path.toString());
		}
		
		return set;
	}
	
	public Calendar getCalendar() {
		return calendar;
	}

	public void setCalendar(Calendar calendar) {
		this.calendar = calendar;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public List<LogTypes> getLogTypes() {
		return logTypes;
	}

	public void setLogTypes(List<LogTypes> logTypes) {
		this.logTypes = logTypes;
	}

	public void setPaths(Set<String> paths) {
		this.paths = paths;
	}

	public void setGranularity(Granularity granularity) {
		this.granularity = granularity;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public void setPreProcessEngine(PreProcessEngine preProcessEngine) {
		this.preProcessEngine = preProcessEngine;
	}

}
