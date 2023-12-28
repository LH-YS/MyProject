package com.neuedu.neuedu.just_demo2;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo3 {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(configuration);
			Path dstPath = new Path("/demo/Htest2.txt");
			FSDataInputStream inputStream = hdfs.open(dstPath);
			
			//显示文件内容
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			String lineString = reader.readLine();
			while(null != lineString) {
				System.out.println(lineString);
				lineString = reader.readLine();
			}
			reader.close();
			inputStream.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
}