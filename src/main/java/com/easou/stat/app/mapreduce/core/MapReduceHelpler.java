package com.easou.stat.app.mapreduce.core;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @ClassName: MapReduceHelpler
 * @Description: mr的帮助类，提供一些工具方法
 * @author 廖瀚卿
 * @date 2012-8-16 下午06:19:16
 * 
 */
public class MapReduceHelpler {

	/**
	 * @Title: outputKeyValue
	 * @Description: 输出键值
	 * @param k_code
	 * @param k_value
	 * @param v_str
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author 廖瀚卿
	 * @return void
	 * @throws
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void outputKeyValue(String k_code, String k_value, String v_str, Context context) throws IOException, InterruptedException {
		TextKey key = new TextKey(new Text(k_code), new Text(k_value));
		context.write(key, new Text(v_str));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void outputKeyValueAsc(String k_code, String k_value, String v_str, Context context) throws IOException, InterruptedException {
		TextKeyAsc key = new TextKeyAsc(new Text(k_code), new Text(k_value));
		context.write(key, new Text(v_str));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void outputLongKeyValue(String k_code, long k_value, String v_str, Context context) throws IOException, InterruptedException {
		LongKey key = new LongKey(new Text(k_code), new LongWritable(k_value));
		context.write(key, new Text(v_str));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void outputLongKeyValueAsc(String k_code, long k_value, String v_str, Context context) throws IOException, InterruptedException {
		LongKeyAsc key = new LongKeyAsc(new Text(k_code), new LongWritable(k_value));
		context.write(key, new Text(v_str));
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void outputIntKeyValue(String k_code, int k_value, String v_str, Context context) throws IOException, InterruptedException {
		IntKey key = new IntKey(new Text(k_code), new IntWritable(k_value));
		context.write(key, new Text(v_str));
	}
}
