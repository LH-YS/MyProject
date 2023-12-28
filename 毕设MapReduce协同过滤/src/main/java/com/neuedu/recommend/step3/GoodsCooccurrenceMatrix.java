package com.neuedu.recommend.step3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class GoodsCooccurrenceMatrix {
	//根据第二步得到的数据计算音乐的共现次数
	//输入数据应该是 mid mid
	
	
	public static class GoodsCooccurrenceMatrixMapper extends Mapper<Text, NullWritable, Text, Text>{
		private Text outK = new Text();
		private Text outV = new Text();
		
		@Override
		protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException{
			String[] line = key.toString().split("\t");
			outK.set(line[0]);
			outV.set(line[1]);
			context.write(outK, outV);
		}
		
	}
	public static class GoodsCooccurrenceMatrixReducer extends Reducer<Text, Text, Text, Text>{
		//定义一个map来存储输出的键信息
		private Map<String, Integer> map = new HashMap<String, Integer>();
        private StringBuffer sb = new StringBuffer();
        private Text outV = new Text();
		//此时输入的数据为 mid [mid,mid,...]
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text val : values) {
                if (map.containsKey(val.toString())){
                    //如果map中包含该键
                    map.put(val.toString(),map.get(val.toString())+1);
                }else {
                    map.put(val.toString(),1);
                }
            }
			//拼接字符串
			for (Map.Entry<String, Integer> en : map.entrySet()) {
                sb.append(en.getKey()).append(":").append(en.getValue()).append(",");
            }
			//去除末尾的
			sb.setLength(sb.length()-1);
            outV.set(sb.toString());

            context.write(key,outV);

            sb.setLength(0);
            map.clear();
            outV.clear();
			
			//输出结果为 min mid：num。mid：num，。。。
		}
		
	}
	
}
