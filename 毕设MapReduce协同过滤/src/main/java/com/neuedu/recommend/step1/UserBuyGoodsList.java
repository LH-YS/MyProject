package com.neuedu.recommend.step1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.sound.sampled.Line;
import javax.xml.transform.OutputKeys;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//计算用户收听音乐列表
//结果应该是 uid  mid1,mid2

public class UserBuyGoodsList {
	//单行 uid mid time
	public static class UserBuyGoodsListMapper extends Mapper<LongWritable,Text,Text,Text>{
		private Text outK = new Text();
		private Text outV = new Text();
		@Override
		protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException {
			String[] line=value.toString().split(",");
			outK.set(line[1]);
			outV.set(line[2]);
			context.write(outK, outV);
		}
		
	}
	public static class UserBuyGoodsListReducer extends Reducer<Text, Text, Text, Text> {
		//结果数据应该为 uid mid1.mid2.。。
		private Text outV = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			StringBuffer sb = new StringBuffer();
			for(Text value:values) {
				//字符串拼接
				sb.append(value.toString()+",");
				
			}
			//将字符串最后的，去掉
			sb.setLength(sb.length()-1);
			outV.set(sb.toString());
			context.write(key, new Text(sb.toString()));
			outV.clear();
			
			
			
		}
		
	}
}
