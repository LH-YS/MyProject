package com.neuedu.recommend.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class GoodsPartitioner extends Partitioner<GoodsBean, Text> {
	//mapreduce分区类
	@Override
    public int getPartition(GoodsBean goodsBean, Text text, int numPartitions) {
        return Math.abs(Integer.parseInt(goodsBean.getG_id()) * 127) % numPartitions;
    }

}
