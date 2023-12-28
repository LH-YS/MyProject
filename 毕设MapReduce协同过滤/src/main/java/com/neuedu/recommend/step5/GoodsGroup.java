package com.neuedu.recommend.step5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GoodsGroup extends WritableComparator{
	public GoodsGroup() {
		super(GoodsBean.class,true);
	}
	
	@Override
	public int compare(WritableComparable a,WritableComparable b) {
		//基于音乐的id分组，id相同分为一组
		GoodsBean o=(GoodsBean) a;
		GoodsBean o1=(GoodsBean) b;
		return o.getG_id().compareTo(o1.getG_id());
	}
	
}
