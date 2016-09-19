package com.easou.stat.app.mapreduce.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.easou.stat.app.constant.Constant;

public class DimUtil {
	
	public static List<String> composeDims(List<String> prefixs, String dim,
			boolean isAll) {
		List<String> list = new ArrayList<String>();
		if (prefixs == null || prefixs.size() == 0) {
			list.add(dim);
			if (isAll)
				list.add("all");
		} else {
			for (String prefix : prefixs) {
				list.add(prefix + Constant.DELIMITER + dim);
				if (isAll)
					list.add(prefix + Constant.DELIMITER + "all");
			}
		}
		return list;
	}

	public static List<String> composeDims(List<String> prefixs, String dim,
			String dimCode, boolean isAll) {
		List<String> list = new ArrayList<String>();
		dimCode = StringUtils.trimToNull(dimCode);
		if (prefixs == null || prefixs.size() == 0) {
			list.add(dim + Constant.DELIMITER + dimCode);
			if (isAll)
				list.add("all");
		} else {
			for (String prefix : prefixs) {
				if (StringUtils.isNotEmpty(dimCode))
					list.add(prefix + Constant.DELIMITER + dim
							+ Constant.DELIMITER + dimCode);
				if (isAll)
					list.add(prefix + Constant.DELIMITER + dim
							+ Constant.DELIMITER + "all");
			}
		}
		return list;
	}
}
