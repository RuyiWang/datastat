package com.easou.stat.app.mapreduce.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LongKeyAsc extends Key<LongWritable> {
	/**
	 * @ClassName: GroupingComparator
	 * @Description: map和reduce的key分组比较器
	 * @author moon
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(LongKeyAsc.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			LongKeyAsc k1 = (LongKeyAsc) a;
			LongKeyAsc k2 = (LongKeyAsc) b;
			return k1.getDimension().compareTo(k2.getDimension());
		}
	}

	public LongKeyAsc() {
		this.dimension = new Text();
		this.value = new LongWritable(0);
	}

	public LongKeyAsc(Text dimension, LongWritable value) {
		super();
		this.dimension = dimension;
		this.value = value;
	}

	@Override
	public int compareTo(Object o) {
		if (o instanceof LongKeyAsc) {
			LongKeyAsc tp = (LongKeyAsc) o;
			int cmp = this.dimension.compareTo(tp.dimension);
			if (cmp != 0) {
				return cmp;
			}
			if (!this.value.equals(tp.getValue())) {
				return this.value.get() > tp.getValue().get() ? 1 : -1;//升序
			}
		}
		return 0;
	}

	@Override
	public Text getDimension() {
		return dimension;
	}

	@Override
	public LongWritable getValue() {
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
	public void setValue(LongWritable value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.dimension.write(out);
		this.value.write(out);
	}
}
