package com.easou.stat.app.constant;

/**
 * @ClassName: LogTypes
 * @Description: 日志类型
 * @author 张羽强
 * @date 2012-9-4 下午6:03:48
 * 
 */
public enum LogTypes {
	M,	//	TYPE_MOBILEPHONEINFO  手机信息数据
	B, 	//	TYPE_BEHAVIORINFO     手机行为数据
	E,	//	TYPE_EXCEPTIONINFO    手机访问异常数据
	A,  //	type_activityinfo     用户访问日志 
	S,	//	type_searchinfo 
	V,  //	type_eventinfo
	TM,	//	中间计算生成日志的Moble日志
	SA,	//	分享日志
	SC;	//	分享点击日志
	

	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}
	
	
	public String getDefaultValue() {
		switch (this) {
		case M:
			return Constant.TYPE_MOBILEPHONEINFO;
		case B:
			return Constant.TYPE_BEHAVIORINFO;
		case E:
			return Constant.TYPE_EXCEPTIONINFO;
		case A:
			return Constant.TYPE_ACTIVITYINFO;
		case S:
			return Constant.TYPE_SEARCHINFO;
		case V:
			return Constant.TYPE_EVENTINFO;
		case TM:
			return Constant.TMP_MOBILEPHONEINFO;
		case SA:
			return Constant.TMP_MOBILEPHONEINFO;
		case SC:
			return Constant.TMP_MOBILEPHONEINFO;
		default:
			return "";
		}
	}
}