package com.neuedu.recommend.step5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class MultiplyGoodsMatrixAndUserVector {
	//这一步将音乐共现矩阵乘以用户行为向量，形成临时推荐成果
	public static class MultiplyGoodsMatrixAndUserVectorFirstMapper extends Mapper<Text, Text, GoodsBean, Text>{
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			//map输出的数据为 GoodsBean(mid,1) mid:num,...
			context.write(new GoodsBean(key.toString(), 1), value);
		}
	}
	
	//输入数据 用户购买向量 mid uid：1，uid：1，。。。
	 public static class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<Text, Text, GoodsBean, Text>{
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			//map输出的数据应为 GoodsBean(mid,0)
			context.write(new GoodsBean(key.toString(), 0), value);
		}
	}
	//期望输出数据 uid，mid 2
	 public static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<GoodsBean, Text, Text, DoubleWritable>{
		//进入reduce的数据为 goodsbean（mid,1） goodsbean（mid,0）  mid:num,mid:num
		@Override
		protected void reduce(GoodsBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Iterator<Text> iter = values.iterator();
			//拿到项 mid:2,mid:3,...
			String[] goods = iter.next().toString().split(",");
			while(iter.hasNext()) {
				//拿到用户购买向量 uid:1，uid:1
				String[] users = iter.next().toString().split(",");
				for (String user : users) {
                    String[] uid_nums = user.split(":");
					
                    for (String good : goods) {
                        String[] gid_nums = good.split(":");
                        //sb作为key输出
                        StringBuffer sb = new StringBuffer();

                        sb.append(uid_nums[0]).append(",").append(gid_nums[0]);

                        context.write(new Text(sb.toString()), new DoubleWritable(Double.parseDouble(uid_nums[1]) * Double.parseDouble(gid_nums[1])));

                        sb.setLength(0);
					}
				}
			}
		}
	}
	
	
}
