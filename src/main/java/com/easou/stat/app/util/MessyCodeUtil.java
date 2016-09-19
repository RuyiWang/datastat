package com.easou.stat.app.util;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessyCodeUtil {
	public static boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	public static boolean isMessyCode(String strName) {
		Pattern p = Pattern.compile("\\s*|\t*|\r*|\n*");
		Matcher m = p.matcher(strName);
		String after = m.replaceAll("");
		String temp = after.replaceAll("\\p{P}", "");
		char[] ch = temp.trim().toCharArray();
		float chLength = ch.length;
		float count = 0;
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (!Character.isLetterOrDigit(c)) {

				if (!isChinese(c)) {
					count = count + 1;
				}
			}
		}
		float result = count / chLength;
	//	System.out.println(result);
		if (result > 0.5) {
			return true;
		} else {
			return false;
		}

	}

	public static String changeStrCode(String phoneCity){
		if(phoneCity.contains("省")||phoneCity.contains("区")||
				phoneCity.contains("自治")||phoneCity.contains("市")
				||phoneCity.contains("县")||phoneCity.contains("州")||
				phoneCity.contains("直辖")){
			return phoneCity.replaceAll(",", "");
		}else{
			byte[] strByte;
			String str; 
			try {
				strByte = phoneCity.getBytes("iso-8859-1");
				str = new String(strByte,"UTF-8");
			} catch (UnsupportedEncodingException e) {
				return "其他";
			}
			if(str.contains("省")||str.contains("区")||
					phoneCity.contains("自治")||str.contains("市")
					||str.contains("县")||str.contains("州")||
					str.contains("直辖")){
				return str.replaceAll(",", "");
			}else{
				return "其他";
			}
			
		}
	}
	public static void main(String[] args) {
		//å¹¿ä¸çå¹¿å·å¸ç½äºåºg107
	//	System.out.println(isMessyCode("å¹¿ä¸çå¹¿å·å¸ç½äºåºg107"));
		String str = "äºåçææå¸å®æ¸¡åºg324";
		System.out.println(changeStrCode(str));
	//	System.out.println(isMessyCode("你好"));
	}
}
