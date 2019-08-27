package com.round.bigdata.hadoop.mr.wc;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-27 14:03
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 使用MR统计HDFS 上的文件对应的词频统计
 */
public class WordCountApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop000:8020");

        // 创建Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数:主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job对应的参数: 设置自定义的Mapper和Reduce 处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应的参数: Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输出目录如果存在, 则先删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "root");
        Path output = new Path("/wordcount/output");
        if (fileSystem.exists(output)) {
            fileSystem.delete(output,true);
        }


        // 设置Job对应的参数: Mapper输出key和value 的类型: 作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/input"));
        FileOutputFormat.setOutputPath(job, output);

        // 提交Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
