package com.easou.stat.app.mapreduce.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: IntKey
 * @Description: value值类型为整形的二次排序key
 * @author 廖瀚卿
 * @date 2012-8-21 下午02:03:10
 * 
 */
public class IntKey extends Key<IntWritable> {
	/**
	 * @ClassName: GroupingComparator
	 * @Description: map和reduce的key分组比较器
	 * @author 廖瀚卿
	 * @date 2012-8-16 下午05:57:54
	 * 
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(IntKey.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			IntKey k1 = (IntKey) a;
			IntKey k2 = (IntKey) b;
			return k1.getDimension().compareTo(k2.getDimension());
		}
	}

	public IntKey() {
		this.dimension = new Text();
		this.value = new IntWritable(0);
	}

	public IntKey(Text dimension, IntWritable value) {
		super();
		this.dimension = dimension;
		this.value = value;
	}

	@Override
	public int compareTo(Object o) {
		if (o instanceof IntKey) {
			IntKey tp = (IntKey) o;
			int cmp = this.dimension.compareTo(tp.dimension);
			if (cmp != 0) {
				return cmp;
			}
			if (!this.value.equals(tp.getValue())) {
				return this.value.get() > tp.getValue().get() ? -1 : 1; //降序
			}
		}
		return 0;
	}

	@Override
	public Text getDimension() {
		return dimension;
	}

	@Override
	public IntWritable getValue() {
		return value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.dimension.readFields(in);
		this.value.readFields(in);
	}

	@Override
	public void setDimension(Text dimension) {
		this.dimension = dimension;
	}

	@Override
	public void setValue(IntWritable value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.dimension.write(out);
		this.value.write(out);
	}
}
