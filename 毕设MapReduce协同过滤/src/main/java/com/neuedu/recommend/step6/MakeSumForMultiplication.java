package com.neuedu.recommend.step6;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MakeSumForMultiplication {
	//对第五步的计算结果进行求和
	public static class MakeSumForMultiplicationMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable>{
		//map读入的数据为 uid：mid  3
		@Override
		protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException{
			context.write(key, value);
		}
	}
	
	public static class MakeSumForMultiplicationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }
	
}
