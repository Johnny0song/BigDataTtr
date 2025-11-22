package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BeanReduce extends Reducer<LongWritable, FlowBean, LongWritable, FlowBean> {
    private FlowBean result = new FlowBean();

    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws java.io.IOException, InterruptedException {
        Long sumUpFlow = 0L;
        Long sumDownFlow = 0L;
        // Sum up all the upFlow and downFlow for the given phone number (key)
        for (FlowBean val : values) {
            sumUpFlow += val.getUpFlow();
            sumDownFlow += val.getDownFlow();
        }
        result.setUpFlow(sumUpFlow);
        result.setDownFlow(sumDownFlow);
        result.setSumFlow(sumUpFlow + sumDownFlow);

        // Emit the phone number and its total FlowBean
        context.write(key, result);
    }
}
