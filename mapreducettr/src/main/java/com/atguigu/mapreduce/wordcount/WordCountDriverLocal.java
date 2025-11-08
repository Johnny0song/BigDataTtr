package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriverLocal {
    public static void main(String[] args) throws IOException {
        System.out.println("This is the WordCountDriver main method.");
        System.setProperty("HADOOP_USER_NAME", "atguigu");  // 或者有权限的用户

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "WordCountDriver");

        job.setJarByClass(WordCountDriverLocal.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setMapOutputValueClass(org.apache.hadoop.io.IntWritable.class);
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://mycluster/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://mycluster/output"));
        try {
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
