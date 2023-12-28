package com.neuedu.neuedu.just_demo2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Demo4 {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		try {
			FileSystem hdFileSystem = FileSystem.get(configuration);
			Path dstPath =new Path("/demo");
			//遍历指定目录下所有文件与子目录
			for(FileStatus fStatus :hdFileSystem.listStatus(dstPath)) {
				//是否是文件
				if(fStatus.isFile()) {
					FSDataInputStream inputStream = hdFileSystem.open(fStatus.getPath());
					BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
					String line = reader.readLine();
					while(null!=line) {
						System.out.println(line);
						line = reader.readLine();
					}
					reader.close();
					inputStream.close();
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
}
