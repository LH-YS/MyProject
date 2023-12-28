package com.neuedu.recommend;

import com.neuedu.recommend.step8.DataInDB;
import com.neuedu.recommend.step8.RecommendResultBean;
import com.neuedu.recommend.step5.GoodsBean;
import com.neuedu.recommend.step5.GoodsGroup;
import com.neuedu.recommend.step5.GoodsPartitioner;
import com.neuedu.recommend.step5.MultiplyGoodsMatrixAndUserVector;
import com.neuedu.recommend.step4.UserBuyGoodsVector;
import com.neuedu.recommend.step1.UserBuyGoodsList;
import com.neuedu.recommend.step7.DuplicateDataForResult;
import com.neuedu.recommend.step7.UserAndGoods;
import com.neuedu.recommend.step6.MakeSumForMultiplication;
import com.neuedu.recommend.step3.GoodsCooccurrenceMatrix;
import com.neuedu.recommend.step2.GoodsCooccurrenceList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FinalJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		//第一步计算结果的job
		Configuration conf = getConf();
		Path input = new Path("/user/hdfs/recommend2/part-m-00000");
        Path one_output = new Path("/user/recommendresult/goods/out11");
        Path two_output = new Path("/user/recommendresult/goods/out22");
        Path three_output = new Path("/user/recommendresult/goods/out33");
        Path four_output = new Path("/user/recommendresult/goods/out44");
        Path five_output = new Path("/user/recommendresult/goods/out55");
        Path six_output = new Path("/user/recommendresult/goods/out66");
        Path seven_output = new Path("/user/recommendresult/goods/out77");
        Path eight_output = new Path("/user/recommendresult/goods/out88");
        
		FileSystem fs = FileSystem.get(conf);
		//判断输出路径是否存在否则就删除
		if (fs.exists(one_output)) {
            fs.delete(one_output, true);
        }
        if (fs.exists(two_output)) {
            fs.delete(two_output, true);
        }
        if (fs.exists(three_output)) {
            fs.delete(three_output, true);
        }
        if (fs.exists(four_output)) {
            fs.delete(four_output, true);
        }
        if (fs.exists(five_output)) {
            fs.delete(five_output, true);
        }
        if (fs.exists(six_output)) {
            fs.delete(six_output, true);
        }
        if (fs.exists(seven_output)) {
            fs.delete(seven_output, true);
        }
        if (fs.exists(eight_output)) {
            fs.delete(eight_output, true);
        }
		
		//第一步 计算用户收听音乐的列表
        Job job =Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Step1:计算用户收听音乐的列表");
        
        job.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextInputFormat.addInputPath(job, input);
        SequenceFileOutputFormat.setOutputPath(job, one_output);
        //计算第二步计算结果的job
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(this.getClass());
        job1.setJobName("Step2:计算音乐的共现关系");
		
        job1.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job1, one_output);
        SequenceFileOutputFormat.setOutputPath(job1, two_output);
        
        //第三步计算结果的job任务
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(this.getClass());
        job2.setJobName("Step3:计算音乐的共现次数(共现矩阵)");

        job2.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);


        job2.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job2, two_output);
        SequenceFileOutputFormat.setOutputPath(job2, three_output);
		
        //step4计算用户购买向量
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(this.getClass());
        job3.setJobName("Step3:计算用户的收听向量");

        job3.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);


        job3.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        //读取源文件数据
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(job3, input);
        SequenceFileOutputFormat.setOutputPath(job3, four_output);
		
        //第五步计算结果   商品共现矩阵乘以用户购买向量
        Job job4 = Job.getInstance(conf);
        job4.setJarByClass(this.getClass());
        job4.setJobName("Step4:音乐共现矩阵乘以用户收听向量，形成临时的推荐结果");

        // 构建多个不同的map任务
        //商品共现次数mapper
        MultipleInputs.addInputPath(job4,
                three_output,
                SequenceFileInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorFirstMapper.class);

        //用户购买向量mapper
        MultipleInputs.addInputPath(job4,
                four_output,
                SequenceFileInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorSecondMapper.class);

        job4.setMapOutputKeyClass(GoodsBean.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setPartitionerClass(GoodsPartitioner.class);
        job4.setGroupingComparatorClass(GoodsGroup.class);

        job4.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);

        job4.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job4, five_output);

        //第六步：对第5步计算的推荐的零散结果进行求和
        Job job5 = Job.getInstance(conf);
        job5.setJarByClass(this.getClass());
        job5.setJobName("Step6:对第5步计算的推荐的零散结果进行求和");

        job5.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(DoubleWritable.class);

        job5.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(DoubleWritable.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job5, five_output);
        SequenceFileOutputFormat.setOutputPath(job5, six_output);
        
        //第七步
        Job job6 = Job.getInstance(conf);
        job6.setJarByClass(this.getClass());
        job6.setJobName("Step7:数据去重，在推荐结果中去掉用户已购买的商品信息");

        // 构建多个不同的map任务
        //FirstMapper处理用户的购买列表数据。
        MultipleInputs.addInputPath(job6,
                input,
                TextInputFormat.class,
                DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);

        //SecondMapper处理第6的推荐结果数据。
        MultipleInputs.addInputPath(job6,
                six_output,
                SequenceFileInputFormat.class,
                DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);

        job6.setMapOutputKeyClass(UserAndGoods.class);
        job6.setMapOutputValueClass(Text.class);

        //设置分组
        // job6.setGroupingComparatorClass(DuplicateDataGroup.class);

        job6.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(NullWritable.class);

        job6.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job6, seven_output);
        
        //第8步：将推荐结果保存到MySQL数据库中     数据来源于第七步
        Job job7 = Job.getInstance(conf);
        job7.addFileToClassPath(new Path("/user/mysql-connector-java-5.1.28.jar"));
        job7.setJarByClass(this.getClass());
        job7.setJobName("Step8:将推荐结果保存到MySQL数据库中");
        
        DBConfiguration.configureDB(job7.getConfiguration(), "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.245.128:3306/ssmmusic", "root", "hadoop");
        DBOutputFormat.setOutput(job7, "recommend", "uid", "gid", "nums");

        job7.setMapperClass(DataInDB.DataInDBMapper.class);
        job7.setMapOutputKeyClass(RecommendResultBean.class);
        job7.setMapOutputValueClass(NullWritable.class);

        job7.setReducerClass(DataInDB.DataInDBReducer.class);
        job7.setOutputKeyClass(RecommendResultBean.class);
        job7.setOutputValueClass(NullWritable.class);

        job7.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job7, seven_output);
        job7.setOutputFormatClass(DBOutputFormat.class);
        
        //final: 构建job作业流
        ControlledJob contro_job = new ControlledJob(conf);
        contro_job.setJob(job);

        ControlledJob contro_job1 = new ControlledJob(conf);
        contro_job1.setJob(job1);
        contro_job1.addDependingJob(contro_job);

        ControlledJob contro_job2 = new ControlledJob(conf);
        contro_job2.setJob(job2);
        contro_job2.addDependingJob(contro_job1);

        ControlledJob contro_job3 = new ControlledJob(conf);
        contro_job3.setJob(job3);

        ControlledJob contro_job4 = new ControlledJob(conf);
        contro_job4.setJob(job4);
        contro_job4.addDependingJob(contro_job2);
        contro_job4.addDependingJob(contro_job3);

        ControlledJob contro_job5 = new ControlledJob(conf);
        contro_job5.setJob(job5);
        contro_job5.addDependingJob(contro_job4);

        ControlledJob contro_job6 = new ControlledJob(conf);
        contro_job6.setJob(job6);
        contro_job6.addDependingJob(contro_job5);

        ControlledJob contro_job7 = new ControlledJob(conf);
        contro_job7.setJob(job7);
        contro_job7.addDependingJob(contro_job6);

        JobControl jobs = new JobControl("goods_recommends");
        jobs.addJob(contro_job);
        jobs.addJob(contro_job1);
        jobs.addJob(contro_job2);
        jobs.addJob(contro_job3);
        jobs.addJob(contro_job4);
        jobs.addJob(contro_job5);
        jobs.addJob(contro_job6);
        jobs.addJob(contro_job7);

        Thread t = new Thread(jobs);
        t.start();

        //打印日志
        while (true) {
            for (ControlledJob c : jobs.getRunningJobList()) {
                c.getJob().monitorAndPrintJob();
            }
            if (jobs.allFinished()) break;
        }
        
        
        
		// TODO Auto-generated method stub
		return 0;
	}
	public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new FinalJob(), args));
        	new ToolRunner().run(new FinalJob(), args);
    }
}
