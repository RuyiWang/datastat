package com.easou.stat.app.constant;


/**
* @ClassName: AppKey
* @Description: 客户端key值枚举
* @author 张羽强
* @date 2012-9-4 下午4:40:23
*
*/
public enum AppKey {
	phone_udid,
	appkey,
	cpid,
	type,
	time,
	server_time,
	phone_city,
	phone_apn,
	phone_mac,
	phone_imsi,
	phone_imei,
	phone_model,
	phone_resolution, 
	phone_softname,
	phone_softversion,
	phone_firmware_version, 
	phone_first_run,
	currentnetworktype,
	phoneStart,
	os,
	sdk_version,
	traffic,
	salechannel,
	behavior,
	exception,
	activities,
	phone_esid;
	
	public String ordinalString() {
		return String.valueOf(this.ordinal());
	}
}
