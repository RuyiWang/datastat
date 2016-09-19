package com.easou.stat.app.mapreduce.core;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class TreeMapComparator implements Comparator<TreeMapKey>{

	@Override
	public int compare(TreeMapKey o1, TreeMapKey o2) {
		int cmp =  o1.getValue() > o2.getValue() ? 1 : -1;
		if (cmp == -1) {
			if(o1.getValue() == o2.getValue()){
				return o1.getDimission().compareTo(o2.getDimission());
			}else{
				return -1;
			}
		}
		return cmp;
	}
	
}
