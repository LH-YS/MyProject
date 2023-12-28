package com.neuedu.recommend.step8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DataInDB {
	//结果保存我数据库
	public static class DataInDBMapper extends Mapper<Text, NullWritable, RecommendResultBean, NullWritable> {
        @Override
        protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            String[] line = key.toString().split("\t");
            int k1 = 0,k2=0;
//            for(String line1:line) {
//            	line[k1].trim();
//            	k1++;
//            }
            RecommendResultBean outK = new RecommendResultBean();
            outK.setNums(Double.parseDouble(line[1].trim()));
            
            String[] split = line[0].split(",");
//            for(String line1:split) {
//            	split[k2].trim();
//            	k2++;
//            }
            outK.setUid(Integer.parseInt(split[0].trim()));
           // outK.setUid(1);
            outK.setGid(Integer.parseInt(split[1].trim()));

            context.write(outK, NullWritable.get());
        }
    }

    public static class DataInDBReducer extends Reducer<RecommendResultBean, DoubleWritable, RecommendResultBean, NullWritable> {
        @Override
        protected void reduce(RecommendResultBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
	
}
