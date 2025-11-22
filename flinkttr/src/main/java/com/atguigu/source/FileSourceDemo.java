package com.atguigu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.impl.FileRecordFormatAdapter;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        // 方式2：设置 Hadoop 配置目录
        System.setProperty("HADOOP_USER_NAME", "atguigu");  // 或者有权限的用户
        System.setProperty("HADOOP_CONF_DIR", "/opt/module/hadoop-3.4.2//etc/hadoop");
        System.setProperty("HADOOP_HOME", "/opt/module/hadoop-3.4.2/");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(
//                "flinkttr/data/input/wrod.txt"
                "hdfs://hadoop101:8020/input/word.txt"
                ))
                .build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source").print();

        env.execute();

    }
}
