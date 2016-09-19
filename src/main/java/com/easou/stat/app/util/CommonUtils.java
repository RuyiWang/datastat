package com.easou.stat.app.util;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.easou.stat.app.constant.Constant;
import com.easou.stat.app.constant.Indicators;
import com.easou.stat.app.mapreduce.client.UserSearchWordMapReduce;
import com.easou.stat.app.schedule.util.JobUtils;

/**
 * @ClassName: CommonUtils
 * @Description: 常用方法工具类
 * @author 张羽强
 * @date 2012-9-3 下午4:06:32
 * 
 */
public final class CommonUtils {
	private static final Log	LOG	= LogFactory.getLog(CommonUtils.class);

	public static String getDateTime(String d, String dFormat) {
		return getDateTime(Long.parseLong(d), dFormat);
	}
	public static String getDateTime(long d, String dFormat) {
		Date dt = new Date(d);
		SimpleDateFormat SNsdf = new SimpleDateFormat(dFormat);
		String SNsystemDate = SNsdf.format(dt); // 得到精确到秒的表示：08/31/2006 21:08:00
		return SNsystemDate;
	}

	/**
	 * @Title: loadXMLFile
	 * @Description: 加载xml配置文件
	 * @param fileName
	 * @return
	 * @author 廖瀚卿
	 * @return XMLConfiguration
	 * @throws
	 */
	public static XMLConfiguration loadXMLFile(String fileName) {
		XMLConfiguration c = null;
		try {
			if (c == null) {
				String pathForConfig = System.getProperty("user.dir") + "/" + fileName;
				c = new XMLConfiguration(pathForConfig);
			}
		} catch (Exception e) {
			if (c == null) {
				URL urlConfig = Thread.currentThread().getContextClassLoader().getResource(fileName);
				try {
					c = new XMLConfiguration(urlConfig);
				} catch (ConfigurationException e1) {
					e1.printStackTrace();
				}
			}
		}
		return c;
	}

	/**
	 * @Title: isNumeric
	 * @Description: 必须是数字类型,并且每个字符都是数字没有空格,没有小数点
	 * @param str
	 * @author 廖瀚卿
	 * @return boolean
	 * @throws
	 */
	public static boolean isNumeric(String str) {
		return (StringUtils.isNotBlank(str) && StringUtils.isNumeric(str));
	}

	/**
	 * @Title: getCalendarByStringOfDateTime
	 * @Description: 根据字符从日期，返回Calendar对象 yyyMMddHHmm
	 * @param dateTime
	 * @throws ParseException
	 * @author 廖瀚卿
	 * @return Calendar
	 * @throws
	 */
	public static Calendar getCalendarByStringOfDateTime(String dateTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		Date d = sdf.parse(dateTime);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		return c;
	}

	/**
	 * @Title: getBeforeDayCalendarByStringOfDateTime
	 * @Description: 根据字符从日期，返回前一天 Calendar对象 yyyMMddHHmm
	 * @param dateTime
	 * @throws ParseException
	 * @author sliven
	 * @return Calendar
	 * @throws
	 */

	public static Calendar getBeforeDayCalendarByStringOfDateTime(String dateTime) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		java.util.Date utilDate = format.parse(dateTime);
		Calendar cal = format.getCalendar();
		cal.setTime(utilDate);
		cal.add(Calendar.DATE, -1);
		cal.setTime(cal.getTime());
		return cal;
	}

	/**
	 * @Title: getDays
	 * @Description: 计算两个日期之间相差的天数
	 * @param strDate1 yyyyMMdd
	 * @param strDate2 yyyyMMdd
	 * @return
	 * @author vic
	 * @return int
	 * @throws
	 */
	public static int getDays(String strDate1, String strDate2) {
		if (null == strDate1 || null == strDate2) {
			return -1;
		}
		int iDay = 0;
		try {
			Date d1 = SDF_DAY.parse(strDate1);
			Date d2 = SDF_DAY.parse(strDate2);
			// 86400000 = 3600 * 1000 * 24
			iDay = (int) ((d2.getTime() - d1.getTime()) / 86400000);
		} catch (ParseException e) {
			iDay = -1;
		}
		return iDay;
	}

	/**
	 * @Title: getWeeks
	 * @Description: 计算两个日期之间相差的周数
	 * @param strDate1 yyyyMMdd
	 * @param strDate2 yyyyMMdd
	 * @return
	 * @author vic
	 * @return int
	 * @throws
	 */
	public static int getWeeks(String strDate1, String strDate2) {
		if (null == strDate1 || null == strDate2) {
			return -1;
		}
		int iWeek;
		Calendar before = Calendar.getInstance();
		Calendar after = Calendar.getInstance();
		try {
			before.setTime(SDF_DAY.parse(strDate1));
			after.setTime(SDF_DAY.parse(strDate2));
			int week = before.get(Calendar.DAY_OF_WEEK);
			before.add(Calendar.DATE, -week);
			week = after.get(Calendar.DAY_OF_WEEK);
			after.add(Calendar.DATE, 7 - week);
			// 604800000 = 3600 * 1000 * 24 * 7
			iWeek = (int) ((after.getTimeInMillis() - before.getTimeInMillis()) / 604800000);
			iWeek = iWeek - 1;
		} catch (ParseException e) {
			iWeek = -1;
		}
		return iWeek;
	}

	/**
	 * @Title: getMonths
	 * @Description: 计算两个日期之间相差的月数
	 * @param strDate1 yyyyMM
	 * @param strDate2 yyyyMM
	 * @return
	 * @author vic
	 * @return int
	 * @throws
	 */
	public static int getMonths(String strDate1, String strDate2) {
		if (null == strDate1 || null == strDate2) {
			return -1;
		}
		int iMonth = 0;
		int flag = 0;
		try {
			Calendar cal1 = Calendar.getInstance();
			cal1.setTime(SDF_MONTH.parse(strDate1));
			Calendar cal2 = Calendar.getInstance();
			cal2.setTime(SDF_MONTH.parse(strDate2));
			if (cal2.equals(cal1))
				return 0;
			// if (cal1.after(cal2)) {
			// Calendar temp = cal1;
			// cal1 = cal2;
			// cal2 = temp;
			// }
			if (cal2.get(Calendar.DAY_OF_MONTH) < cal1.get(Calendar.DAY_OF_MONTH))
				flag = 1;

			if (cal2.get(Calendar.YEAR) > cal1.get(Calendar.YEAR))
				iMonth = ((cal2.get(Calendar.YEAR) - cal1.get(Calendar.YEAR)) * 12 + cal2.get(Calendar.MONTH) - flag) - cal1.get(Calendar.MONTH);
			else
				iMonth = cal2.get(Calendar.MONTH) - cal1.get(Calendar.MONTH) - flag;
		} catch (Exception e) {
			iMonth = -1;
		}
		return iMonth;
	}

	private static SimpleDateFormat	SDF_DAY		= new SimpleDateFormat("yyyyMMdd"); // 天，周格式
	private static SimpleDateFormat	SDF_MONTH	= new SimpleDateFormat("yyyyMM");	// 月格式

	/**
	 * 判断两个时间是否为同一天，前一天，上周，上个月
	 * 
	 * @author vic
	 * @param conditionType 判断条件类型 （0:同一天;1:前一天;2:上周;3:上个月）
	 * @param dateTime （天，周：yyyyMMdd;月：yyyyMM）
	 * @param time 行日志时间 yyyyMMddHHmm
	 * @return [参数说明]
	 * 
	 * @return Boolean [返回类型说明]
	 * @exception throws [违例类型] [违例说明]
	 * @see [类、类#方法、类#成员]
	 */
	public static boolean compareDateTime(int conditionType, String dateTime, String time) {
		Boolean flag = Boolean.FALSE;
		if (!StringUtils.isEmpty(dateTime) && !StringUtils.isEmpty(time)) {
			Calendar cal = Calendar.getInstance();
			Date date = null;
			SimpleDateFormat SDF = null;
			switch (conditionType) {
			case 0:
				// 同一天
				try {
					SDF = SDF_DAY;
					date = SDF.parse(dateTime);
				} catch (ParseException ex) {

				}
				cal.setTime(date);
				cal.add(Calendar.DATE, 0);
				time = time.substring(0, 8);
				break;
			case 1:
				// 前一天
				try {
					SDF = SDF_DAY;
					date = SDF.parse(dateTime);
				} catch (ParseException ex) {

				}
				cal.setTime(date);
				cal.add(Calendar.DATE, -1);
				time = time.substring(0, 8);
				break;
			case 2:
				// 上周
				try {
					SDF = SDF_DAY;
					date = SDF.parse(dateTime);
				} catch (ParseException ex) {

				}
				cal.setTime(date);
				cal.add(Calendar.WEEK_OF_YEAR, -1);
				time = time.substring(0, 8);
				break;
			case 3:
				// 上个月
				try {
					SDF = SDF_MONTH;
					date = SDF.parse(dateTime);
				} catch (ParseException ex) {

				}
				cal.setTime(date);
				cal.add(Calendar.MONDAY, -1);
				time = time.substring(0, 6);
				break;
			default:
				break;
			}
			date = cal.getTime();
			String newDateTime = SDF.format(date);
			// 周
			if (conditionType == 2) {
				try {
					if (isSameWeekDates(date, SDF.parse(time))) {
						flag = Boolean.TRUE;
					}
				} catch (ParseException e) {

				}
			} else {
				// 日，月
				if (time.equals(newDateTime)) {
					flag = Boolean.TRUE;
				}
			}
		}
		return flag;
	}

	/**
	 * @Title: isSameWeekDates
	 * @Description: 判断两个日期是否在同一周
	 * @param date1
	 * @param date2
	 * @return
	 * @author vic
	 * @return boolean
	 * @throws
	 */
	static boolean isSameWeekDates(Date date1, Date date2) {
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		cal1.setTime(date1);
		cal2.setTime(date2);
		int subYear = cal1.get(Calendar.YEAR) - cal2.get(Calendar.YEAR);
		if (0 == subYear) {
			if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR))
				return true;
		} else if (1 == subYear && 11 == cal2.get(Calendar.MONTH)) {
			// 如果12月的最后一周横跨来年第一周的话则最后一周即算做来年的第一周
			if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR))
				return true;
		} else if (-1 == subYear && 11 == cal1.get(Calendar.MONTH)) {
			if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR))
				return true;
		}
		return false;
	}

	/**
	 * @Title: getDistinctValuesMap
	 * @Description: 获得valuea值是set集合到map对象,用来存储需要去重复的数据
	 * @author 廖瀚卿
	 * @return Map<String,Set<String>>
	 * @throws
	 */
	public static Map<Indicators, Set<String>> getDistinctValuesMap() {
		Map<Indicators, Set<String>> map = new HashMap<Indicators, Set<String>>();
		for (Indicators si : Indicators.values()) {
			map.put(si, new HashSet<String>());
		}
		return map;
	}

	public static Map<Indicators, Set<String>> getDistinctValuesMapValueWritable() {
		Map<Indicators, Set<String>> map = new HashMap<Indicators, Set<String>>();
		for (Indicators si : Indicators.values()) {
			map.put(si, new HashSet<String>());
		}
		return map;
	}

	/**
	 * @Title: getTotalValuesMap
	 * @Description: 获得values值是long的map对象
	 * @author 廖瀚卿
	 * @return Map<String,Long>
	 * @throws
	 */
	public static Map<Indicators, Long> getTotalValuesMap() {
		Map<Indicators, Long> map = new HashMap<Indicators, Long>();
		for (Indicators si : Indicators.values()) {
			map.put(si, 0l);
		}
		return map;
	}

	/**
	 * @Title: getUndistinctValuesMap
	 * @Description: 获得不需要去重的集合map
	 * @author 廖瀚卿
	 * @return Map<SummaryIndicators,List<String>>
	 * @throws
	 */
	public static Map<Indicators, List<String>> getUndistinctValuesMap() {
		Map<Indicators, List<String>> map = new HashMap<Indicators, List<String>>();
		for (Indicators si : Indicators.values()) {
			map.put(si, new ArrayList<String>());
		}
		return map;
	}

	/**
	 * @Title: getLongValue
	 * @Description: 字符转成Long
	 * @param t
	 * @author 廖瀚卿
	 * @return Long
	 * @throws
	 */
	public static Long getLongValue(String t) {
		if (StringUtils.isNotEmpty(t)) {
			return Long.valueOf(t);
		}
		return 0l;
	}

	/*
	 * @SuppressWarnings({ "rawtypes", "unchecked" }) public static AbstractLogFormat getLogFormat(String line) { AbstractLogFormat f = null; if(line.startsWith("{") && line.endsWith("}")){ f =
	 * MapReduceHelpler.getLogFormatWithJson(line); }else{ List<String> list = getFields(line); f = getLogFormat0(list); if (f != null) { f.init(list); } } return f; }
	 */
	/**
	 * @Title: getLogFormat
	 * @Description: 根据日志数据创建日志对象
	 * @param line
	 * @return
	 * @author 廖瀚卿
	 * @return AbstractLogFormat
	 * @throws
	 */
	/*
	 * @SuppressWarnings({ "rawtypes", "unchecked" }) public static AbstractLogFormat getLogFormat(String line, Configuration conf) { List<String> list = getFields(line); AbstractLogFormat f = getLogFormat0(list); if (f
	 * != null) { f.setConf(conf); f.init(list); } return f; }
	 */

	/**
	 * @Title: timeToString
	 * @Description: 将一个日期按照制定格式输出字符表现形式
	 * @param date
	 * @param format
	 * @author 廖瀚卿
	 * @return String
	 * @throws
	 */
	public static String timeToString(Date date, String format) {
		if (date == null) {
			return null;
		}
		if (StringUtils.isEmpty(format)) {
			format = "yyyyMMdd";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}

	public static String timeToString(Date date) {
		return timeToString(date, null);
	}

	public static String listToString(Object object) {
		return object.toString().replaceAll("\\[", "").replaceAll("]", "").replaceAll(",", "").replaceAll("\\s", "");
	}

	/**
	 * @Title: descartes
	 * @Description: 计算机笛卡尔积
	 * @param inputList
	 * @author 廖瀚卿
	 * @return List
	 * @throws
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List descartesCode(List... lists) {
		List list = lists[0];
		for (int i = 1; i < lists.length; i++) {
			List otherList = lists[i];
			List temp = new ArrayList();
			for (int j = 0; j < list.size(); j++) {
				for (int k = 0; k < otherList.size(); k++) {
					List cut = new ArrayList();
					if (list.get(j) instanceof List) {
						cut.addAll((List) list.get(j));
					} else {
						cut.add(list.get(j));
					}
					if (otherList.get(k) instanceof List) {
						cut.addAll((List) otherList.get(k));
					} else {
						cut.add(otherList.get(k));
					}
					temp.add(listToString(cut));
				}
			}
			list = temp;
		}
		return list;
	}

	public static List<String> split(String line, String sp) {
		int length = sp.length();
		List<String> fds = new ArrayList<String>();
		int start = 0, end = line.indexOf(sp);
		while (end != -1) {
			fds.add(line.substring(start, end)); // 根据分隔符号截取每一个字段
			start = end + length;
			end = line.indexOf(sp, start);
		}
		fds.add(line.substring(start));
		return fds;
	}

	/**
	 * @Title: getSubStrCount
	 * @Description: 获取某个子字符串在字符串里的数量
	 * @param str
	 * @param regex
	 * @return
	 * @author vic
	 * @return int
	 * @throws
	 */
	public static int getSubStrCount(String str, String regex) {
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(str);
		int matchCount = 0;
		while (m.find()) {
			matchCount++;
		}
		return matchCount;
	}

	/**
	 * @Title: isAllNumber
	 * @Description: 判断给定的字串s是否全为数字。
	 * @param s
	 * @return
	 * @author 邓长春
	 * @return boolean
	 * @throws
	 */
	public static boolean isAllNumber(String s) {
		if (StringUtils.isEmpty(s)) {
			return false;
		} else {
			boolean r = true;
			char[] cArr = s.toCharArray();
			for (char ch : cArr) {
				if (ch < '0' || ch > '9') {
					r = false;
					return r;
				}
			}
			return r;
		}
	}

	/**
	 * @Title: isAllChar
	 * @Description: 判断给定的字串s是否是全部为字符c组成。
	 * @param s
	 * @param c
	 * @return
	 * @author 邓长春
	 * @return boolean
	 * @throws
	 */
	public static boolean isAllChar(String s, char c) {
		if (StringUtils.isEmpty(s)) {
			return false;
		} else {
			boolean r = true;
			char[] cArr = s.toCharArray();
			for (char ch : cArr) {
				if (ch != c) {
					r = false;
					return r;
				}
			}
			return r;
		}

	}

	/**
	 * @Title: filterNotNumberChar
	 * @Description: 过滤掉非数字字符(小数点当做数字字符)。
	 * @param s
	 * @return
	 * @author 邓长春
	 * @return String
	 * @throws
	 */
	public static String filterNotNumberChar(String s) {
		if (StringUtils.isEmpty(s)) {
			return s;
		} else {
			StringBuffer sb = new StringBuffer();
			char[] cArr = s.toCharArray();
			for (char ch : cArr) {
				if (ch != '.' && (ch < '0' || ch > '9')) {
					continue;
				} else {
					sb.append(ch);
				}
			}
			return sb.toString();
		}
	}

	/**
	 * @Title: beCharOfHeadAndTailWithChar
	 * @Description: 判断给定的字符串s是否有以指定的字符c开头或结尾的情况。
	 * @param s
	 * @param c
	 * @return
	 * @author 邓长春
	 * @return boolean
	 * @throws
	 */
	public static boolean beCharOfHeadAndTailWithChar(String s, char c) {
		if (StringUtils.isEmpty(s)) {
			return false;
		}
		char[] cArr = s.toCharArray();
		int l = cArr.length;
		if (cArr[0] == c || cArr[l - 1] == c) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @Title: filterDotOfHeadAndTailWithChar
	 * @Description: TODO 根据给定的字符c过滤掉字串s的首尾（使用了递归）。
	 * @param s
	 * @param c
	 * @return
	 * @author 邓长春
	 * @return String
	 * @throws
	 */
	public static String filterCharOfHeadAndTailWithChar(String s, char c) {
		if (StringUtils.isEmpty(s)) {
			return s;
		}
		char[] cArr = s.toCharArray();
		int b = 0;
		int e = cArr.length - 1;
		if (cArr[b] == c) {
			b = 1;
		}
		if (cArr[e] == c) {
			e = e - 1;
		}
		if (b <= e) {
			String temp = String.valueOf(cArr).substring(b, e + 1);
			if (beCharOfHeadAndTailWithChar(temp, c)) {
				return filterCharOfHeadAndTailWithChar(temp, c);// 使用了递归连续处理
			} else {
				return temp;
			}
		} else {
			return "";
		}
	}

	/**
	 * @Title: truncate
	 * @Description: 对于给定的字串s,根据指定的字符c,过滤掉长途连超过l位c的串
	 * @param s
	 * @param c
	 * @param l
	 * @return
	 * @author 邓长春
	 * @return String
	 * @throws
	 */
	public static String truncate(String s, char c, int l) {
		if (StringUtils.isEmpty(s)) {
			return s;
		} else if (s.length() < l) {
			return s;
		} else {
			char[] cArr = s.toCharArray();
			StringBuffer sb = new StringBuffer();
			int cursor = 0;
			char temp = ' ';
			for (char ch : cArr) {
				if (ch != temp) {
					sb.append(ch);
					temp = ch;
					cursor = 0;
				} else {
					if (cursor < l - 1) {
						sb.append(ch);
						temp = ch;
					}
					cursor++;

				}
			}
			return sb.toString();
		}
	}

	public static String convertModelsToBrand(Map<String, String> map, String models) {
		String brand = "其它";
		if (StringUtils.isNotBlank(models)) {
			brand = map.get(models.trim());
			if (StringUtils.isBlank(brand) || brand.equals("other")) {
				brand = "其它";
			}
		}
		return brand;
	}

	/**
	 * @Title: getDayOfWeek
	 * @Description: 获取日期的周范围
	 * @param dateTime yyyyMMdd
	 * @return yyyyMMdd~yyyyMMdd
	 * @author vic
	 * @return String
	 * @throws
	 */
	public static String getDayOfWeek(String dateTime) {
		String s = "";
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			Date d = sdf.parse(dateTime);
			Calendar c = Calendar.getInstance();
			c.setTime(d);
			c.setFirstDayOfWeek(Calendar.MONDAY);
			c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			String bTime = sdf.format(c.getTime());
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			String eTime = sdf.format(c.getTime());
			s = bTime + "~" + eTime;
		} catch (ParseException e) {
			s = "";
		}
		return s;
	}

	/**
	 * 
	 * @Titile: main
	 * @Description: 
	 * @author: Asa
	 * @date: 2013年10月28日 上午9:58:03
	 * @return void
	 */
	public static void main(String[] args) throws ParseException {
		System.out.println(getMonths("201209", "201212"));
		System.out.println(getWeeks("20121120", "20121127"));
		System.out.println(getDays("20121108", "20121109"));
		System.out.println("==" + "0123456".substring(0, 5) + "==");
		String temp = "中国....0.0..0.0000.0.0.0...0.abcdd.....eeddd.....abc..-中国.c";
		temp = CommonUtils.filterNotNumberChar(temp);
		temp = CommonUtils.truncate(temp, '.', 1);
		temp = temp.substring(0, 5);
		temp = CommonUtils.filterCharOfHeadAndTailWithChar(temp, '.');
		temp = ".....";
		System.out.println("==" + temp + "==");
		System.out.println("==" + isAllNumber(temp) + "==");
		System.out.println("==" + isAllChar(temp, '.') + "==");
		System.out.println(getDayOfWeek("20121113"));
	}
}
