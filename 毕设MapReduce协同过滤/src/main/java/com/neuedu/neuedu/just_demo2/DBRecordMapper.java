package com.neuedu.neuedu.just_demo2;

import java.io.IOException;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 
import com.neuedu.neuedu.just_demo2.DBRecord;
 
@SuppressWarnings("deprecation")
public class DBRecordMapper extends MapReduceBase implements Mapper<LongWritable, DBRecord, LongWritable, Text>{
 
	@Override
	public void map(LongWritable key, DBRecord value,
			OutputCollector<LongWritable, Text> collector, Reporter reporter)
			throws IOException {
		collector.collect(new LongWritable(value.getId()), new Text(value.toString()));  
	}
	
}