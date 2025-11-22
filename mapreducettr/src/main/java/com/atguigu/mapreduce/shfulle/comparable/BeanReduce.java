package com.atguigu.mapreduce.shfulle.comparable;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BeanReduce extends Reducer<FlowBean, LongWritable, LongWritable, FlowBean> {


    @Override
    protected void reduce(FlowBean key, Iterable<LongWritable> values, Reducer<FlowBean, LongWritable, LongWritable, FlowBean>.Context context) throws IOException, InterruptedException {

        for (LongWritable val : values) {
            context.write(val, key);
        }
    }
}
