package com.easou.stat.app.mapreduce.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextKeyAsc extends Key<Text> {

	/**
	 * @ClassName: FirstGroupingComparator
	 * @Description: map和reduce的key分组比较器
	 * @author 廖瀚卿
	 * @date 2012-8-16 下午05:57:54
	 * 
	 */
	public static class GroupingComparator extends WritableComparator {

		protected GroupingComparator() {
			super(TextKeyAsc.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			TextKeyAsc k1 = (TextKeyAsc) a;
			TextKeyAsc k2 = (TextKeyAsc) b;
			return k1.getDimension().compareTo(k2.getDimension());
		}
	}

	public TextKeyAsc() {
		super();
		this.dimension = new Text();
		this.value = new Text();
	}

	public TextKeyAsc(Text dimension, Text value) {
		super();
		this.dimension = dimension;
		this.value = value;
	}

	@Override
	public int compareTo(Object obj) {
		if (obj instanceof TextKeyAsc) {
			TextKeyAsc o = (TextKeyAsc) obj;
			int cmt = this.dimension.compareTo(o.getDimension());
			if (cmt != 0) {
				return cmt;
			}
			return this.value.compareTo(o.getValue());// 升序
		}
		return 0;
	}

	@Override
	public Text getDimension() {
		return dimension;
	}

	@Override
	public Text getValue() {
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
	public void setValue(Text value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.dimension.write(out);
		this.value.write(out);
	}
}
