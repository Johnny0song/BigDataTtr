package com.atguigu.mapreduce.shfulle.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BeanDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        System.out.println("This is the BeanDriver main method.");
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);

        job.setJarByClass(BeanDriver.class);
        job.setMapperClass(BeanMapper.class);
        job.setReducerClass(BeanReduce.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.addInputPath(job,new Path( "/Users/richard/Documents/learningdoc/output/output000"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/richard/Documents/learningdoc/output/output008"));



        try {
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
