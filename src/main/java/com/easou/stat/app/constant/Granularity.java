package com.easou.stat.app.constant;


/**
* @ClassName: Granularity
* @Description: 客户端时间粒度枚举
* @author 张羽强
* @date 2012-9-3 下午3:55:09
*
*/

public enum Granularity {

	// 年、半年、季度、月、周(自然周)、月周、天、小时和10分钟
	YEAR, 
	HALF_YEAR, 
	QUARTER, 
	MONTH, 
	NATURE_WEEK, 
	WEEK, 
	DAY, 
	HOUR, 
	TEN_MINUTES,
	SPIDER_DAY,
	AHOUR,	//一天中当前小时之前的所有小时（包括当前小时）
	DAY7;	//当前天的前7天（包括当前天）
}
