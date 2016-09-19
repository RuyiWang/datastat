package com.easou.stat.app.mapreduce.core;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: FirstPartitioner
 * @Description: 二次排序用到的分区器
 * @author 廖瀚卿
 * @date 2012-8-21 下午05:08:45
 * 
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public class FirstPartitioner<T extends Key> extends Partitioner<T, Text> {
	@Override
	public int getPartition(T key, Text value, int numPartitions) {
		// 当String对象很长时String.hashCode()方法会导致数据溢出,使用& Integer.MAX_VALUE保证不溢出,并且能平坦分布;
		// return Math.abs(key.getDimension().hashCode() * 127) % numPartitions;
		return Math.abs(key.getDimension().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
