package com.neuedu.recommend.step7;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DuplicateDataForResult {
	//数据去重，推荐结果中去掉用户已收听的音乐
	//firstmapper处理用户的音乐列表数据
	public static class DuplicateDataForResultFirstMapper extends Mapper<LongWritable, Text, UserAndGoods, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//flag 1 数据来源源数据 0 来源第六部
			String[] line = value.toString().split(",");

            context.write(new UserAndGoods(line[1], line[2], 1), value);
		}
	}
	//SecondMapper处理第6的推荐结果数据
	public static class DuplicateDataForResultSecondMapper extends Mapper<Text, DoubleWritable, UserAndGoods, Text> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String[] line = key.toString().split(",");

            context.write(new UserAndGoods(line[0], line[1], 0), new Text(key.toString() + "\t" + value.get()));
        }
    }
	//reduce 期望输出的数据为： uid mid 2
	public static class DuplicateDataForResultReducer extends Reducer<UserAndGoods, Text, Text, NullWritable> {
        int i = 0;
        @Override
        protected void reduce(UserAndGoods key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();

            System.out.println((i++) + "--" + key);
            //集合的第一个元素
            Text res = iter.next();
            System.out.println(res.toString());

            //如果集合没有下一个元素，直接写出
            if (!iter.hasNext()) {
                System.out.println("有下一个元素");
                context.write(res, NullWritable.get());
            }

        }
    }
}
