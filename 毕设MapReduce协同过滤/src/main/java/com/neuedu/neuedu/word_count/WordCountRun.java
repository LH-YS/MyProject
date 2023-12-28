package com.neuedu.neuedu.word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountRun {
	public static void main(String[] args) {
		//配置对象
		Configuration configuration = new Configuration();
		try {
			//获取hdfs文件系统
			FileSystem hdfs = FileSystem.get(configuration);
			//定义输入路径
			String inputString = "/books";
			//定义输出路径
			String outputString = "/wordcount_result";
			Path outputPath = new Path(outputString);
			//输出路径不能存在，否则有异常
			if(hdfs.exists(outputPath)) {
				hdfs.delete(outputPath,true);
			}
			//创建job
			Job job = Job.getInstance(configuration);
			//设置jar
			job.setJarByClass(WordCountRun.class);
			//设置输入
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.setInputPaths(job, inputString);
			//设置mapper
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			//设置reducer
			job.setReducerClass(WordCountReaducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			//设置输出
			job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(outputString));
			//运行
			boolean flag = job.waitForCompletion(true);
			if(flag) {
				System.out.println("词频统计结束");
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
