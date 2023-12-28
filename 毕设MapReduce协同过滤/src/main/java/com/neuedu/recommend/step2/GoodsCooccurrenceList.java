package com.neuedu.recommend.step2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//计算音乐的共现关系 结果应为 mid mid
public class GoodsCooccurrenceList {
	//使用sequencefileinputformat读取数据，读入的数据自动基于键和值分割
	public static class GoodsCooccurrenceListMapper extends Mapper<Text, Text, Text, NullWritable> {
		private StringBuffer sb = new StringBuffer();
        private Text outK = new Text();
		
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
			
			//歌曲之间两两组合
            for (String s : line) {
                for (String s1 : line) {
                    sb.append(s).append("\t").append(s1);

                    outK.set(sb.toString());
                    context.write(outK, NullWritable.get());
                    sb.setLength(0);
                    outK.clear();
                }
			}
			
		}
		
		
	}
}
