package com.easou.stat.app.mapreduce.core;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @ClassName: Key
 * @Description: key的抽象类
 * @author 廖瀚卿
 * @date 2012-8-21 下午02:03:38
 * 
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public abstract class Key<T extends WritableComparable> implements WritableComparable {
	protected Text	dimension;
	protected T		value;

	public Text getDimension() {
		return dimension;
	}

	public T getValue() {
		return value;
	}

	public void setDimension(Text dimension) {
		this.dimension = dimension;
	}

	public void setValue(T value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Key [dimension=" + dimension + ", value=" + value + "]";
	}

}
