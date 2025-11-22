package com.atguigu.mapreduce.inputformat.combinetxtinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    private  Text text = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        // Convert the line of text to a String
        String line = value.toString();
        // Split the line into words based on whitespace
        String[] words = line.split("\\s+");
        // For each word, emit (word, 1)
        for (String word : words) {
            text.set(word);
            one.set(1);
            context.write( text, one);
        }
    }
}
