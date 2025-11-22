package com.atguigu.mapreduce.shfulle.partitionercomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner< FlowBean,LongWritable>{


    @Override
    public int getPartition(FlowBean flowBean, LongWritable longWritable,  int numPartitions) {
        Long phoneNum = longWritable.get();
        String phoneStr = phoneNum.toString();
        String prefix = phoneStr.substring(0,3);
        if (prefix.equals("136")){
            return 0;
        }else if (prefix.equals("137")){
            return 1;
        }else if (prefix.equals("138")){
            return 2;
        }else if (prefix.equals("139")){
            return 3;
        }else {
            return 4;
        }
    }
}
