package com.neuedu.neuedu.word_count;

import java.io.IOException;
import java.util.StringTokenizer;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)
		throws IOException,InterruptedException{
		//将Text类型转换成String类型
		String line = value.toString();
		if(StringUtils.isBlank(line)) {
			return;
		}
		//拆分单词：StringTokenizer分装了拆分单词的简单方法
		StringTokenizer stringTokenizer = new StringTokenizer(line);
		while(stringTokenizer.hasMoreTokens()) {
			//取出单词
			String word = stringTokenizer.nextToken();
			//map输出
			context.write(new Text(word), new IntWritable(1));
		}
		
	}
	
}
