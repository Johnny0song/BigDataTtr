package com.atguigu.mapreduce.shfulle.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BeanMapper extends Mapper<LongWritable,Text, FlowBean,LongWritable>{

    private FlowBean outKey = new FlowBean();
    LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, LongWritable>.Context context) throws IOException, InterruptedException {
        // Convert the line of text to a String
        String line = value.toString();
        // Split the line into fields based on tab character
        String[] fields = line.split("\t");
        // Extract phone number, upFlow, and downFlow from the fields
        String phoneNumber = fields[0];
        Long upFlow = Long.parseLong(fields[1]);
        Long downFlow = Long.parseLong(fields[2]);
        Long sumFlow = Long.parseLong(fields[3]);
        outValue.set(Long.parseLong(phoneNumber));
        // Set the FlowBean values
        outKey.setUpFlow(upFlow);
        outKey.setDownFlow(downFlow);
        outKey.setSumFlow(sumFlow);

        context.write(outKey,outValue);

    }
}
