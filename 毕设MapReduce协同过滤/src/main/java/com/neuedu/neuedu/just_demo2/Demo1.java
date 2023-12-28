package com.neuedu.neuedu.just_demo2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo1 {
	public static void main(String[] args) {
		//构建配置对象
		Configuration configuration = new Configuration();
		try {
			//获取hdfs文件系统
			FileSystem hdfs = FileSystem.get(configuration);
			//文件上传，本地文件
			String pathString = "d:/22_0.txt";
			Path srcPath = new Path(pathString);
			//目标文件
			Path dst = new Path("/user/hdfs/22_0.txt");
			//文件上传
			hdfs.copyFromLocalFile(srcPath, dst);
			System.out.println("上传成功");
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
}
