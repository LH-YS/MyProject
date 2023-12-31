package com.neuedu.neuedu.word_count;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WordCountReaducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context)
		throws IOException,InterruptedException{
		int sum = 0;
		//key就是单词、value就是次数的一个集合，所有次数累加就是该单词次数
		for(IntWritable v:values) {
			sum+=v.get();
		}
		//reducer输出
		context.write(key, new IntWritable(sum));
	}
}
