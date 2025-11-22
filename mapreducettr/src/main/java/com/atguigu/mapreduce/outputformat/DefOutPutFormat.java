package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DefOutPutFormat extends FileOutputFormat<LongWritable, FlowBean> {

    @Override
    public RecordWriter<LongWritable, FlowBean> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        return new RecordWriter<LongWritable, FlowBean>() {
            private FSDataOutputStream fsOutputFormatOne;
            private FSDataOutputStream fsOutputFormatTwo;

            // 实例初始化块
            {
                try {
                    // 获取文件系统对象
                    FileSystem fs = FileSystem.get(job.getConfiguration());
                    // 用文件系统对象创建两个输出流对应不同的目录
                    fsOutputFormatOne = fs.create(new Path("mapreducettr/src/main/java/com/atguigu/mapreduce/outputformat/log/log1.log"));
                    fsOutputFormatTwo = fs.create(new Path("mapreducettr/src/main/java/com/atguigu/mapreduce/outputformat/log/log2.log"));
                } catch (IOException e) {
                    throw new RuntimeException("初始化输出流失败", e);
                }
            }

            @Override
            public void write(LongWritable key, FlowBean value) throws IOException {
                long phoneNum = key.get();
                String flowBean = value.toString();

                // 判断手机号的归属地
                if (String.valueOf(phoneNum).startsWith("136")) {
                    fsOutputFormatOne.writeBytes(phoneNum + "\t" + flowBean + "\n");
                } else {
                    fsOutputFormatTwo.writeBytes(phoneNum + "\t" + flowBean + "\n");
                }
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException {
                if (fsOutputFormatOne != null) {
                    fsOutputFormatOne.close();
                }
                if (fsOutputFormatTwo != null) {
                    fsOutputFormatTwo.close();
                }
            }
        };
    }
}
