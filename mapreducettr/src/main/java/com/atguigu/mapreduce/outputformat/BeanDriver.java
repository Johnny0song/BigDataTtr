package com.atguigu.mapreduce.outputformat;

import com.atguigu.mapreduce.shfulle.partitioner.BeanReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutput;
import java.io.IOException;

public class BeanDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        System.out.println("This is the BeanDriver main method.");
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(BeanDriver.class);
        job.setMapperClass(BeanMapper.class);
        job.setReducerClass(BeanReduce.class);

        job.setMapOutputKeyClass(org.apache.hadoop.io.LongWritable.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(org.apache.hadoop.io.LongWritable.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.addInputPath(job,new Path( "/Users/richard/Documents/learningdoc/phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/richard/Documents/learningdoc/output/output002"));

        job.setOutputFormatClass(DefOutPutFormat.class);

        try {
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
