package com.neuedu.neuedu.just_demo2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;

public class Demo5 {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		try {
			FileSystem hdFileSystem = FileSystem.get(configuration);
			String pathString;
			Path outputPath = new Path("/demo/Htest.txt");
			//判断目录或者文件是否存在
			if(hdFileSystem.exists(outputPath)) {
				//存在就删除
				hdFileSystem.delete(outputPath,true);
			}
			System.out.println("删除成功");
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
