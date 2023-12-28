package com.neuedu.neuedu.just_demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo2 {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(configuration);
			byte[] memo = "china very best".getBytes();
			Path dstPath = new Path("/demo/Htest2.txt");
			//构建写入器
			FSDataOutputStream writer = hdfs.create(dstPath);
			//写入数组内容
			writer.write(memo);
			//尤其是循环中必须用flush否则无数据
			writer.flush();
			//关闭流
			writer.close();
			System.out.println("新建文件成功");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
}
