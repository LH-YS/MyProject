package com.neuedu.recommend.step4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UserBuyGoodsVector {
	//该函数计算用户的购买向量
	public static class UserBuyGoodsVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
	        
	        //mid为键 uid为值
	        
	        private Text outK = new Text();
	        private Text outV = new Text();

	        @Override
	        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        	String[] line = value.toString().split(",");

	            outK.set(line[2]);
	            outV.set(line[1]);

	            context.write(outK, outV);
	        }
	    }
	public static class UserBuyGoodsVectorReducer extends Reducer<Text, Text, Text, Text>{
		 //输入值为mid [uid,uid,...]
		private Text outV = new Text();
        private Map<String, Integer> map = new HashMap<>();
        private StringBuffer sb = new StringBuffer();
		 
		 @Override
		 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			 //map的结果是 mid user：num，user：num，。。。
			 for (Text value : values) {
	                if (map.containsKey(value.toString())) {
	                    map.put(value.toString(), map.get(value.toString()) + 1);
	                } else {
	                    map.put(value.toString(), 1);
	                }
	            }
			 
			 for (Map.Entry<String, Integer> en : map.entrySet()) {
	                sb.append(en.getKey()).append(":").append(en.getValue()).append(",");
	            }
	            sb.setLength(sb.length()-1);
	            outV.set(sb.toString());
	            context.write(key,outV);
			 //重置数据
			 sb.setLength(0);
			 map.clear();
			 outV.clear();
		 }
	 }
	
}
