package com.atguigu.mapreduce.shfulle.partitioner;

import com.atguigu.mapreduce.outputformat.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BeanMapper extends Mapper<LongWritable,Text,LongWritable, FlowBean>{
    private LongWritable outKey = new LongWritable();
    private FlowBean outValue = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        // Convert the line of text to a String
        String line = value.toString();
        // Split the line into fields based on tab character
        String[] fields = line.split("\t");
        // Extract phone number, upFlow, and downFlow from the fields
        String phoneNumber = fields[1];
        Long upFlow = Long.parseLong(fields[fields.length - 3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);

        // Set the output key and value
        outKey.set(Long.parseLong(phoneNumber));
        outValue.setUpFlow(upFlow);
        outValue.setDownFlow(downFlow);
        outValue.setSumFlow(upFlow + downFlow);

        // Emit the phone number and its corresponding FlowBean
        context.write(outKey, outValue);
    }
}
