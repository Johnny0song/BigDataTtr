package com.example.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;


public class MyPartitioner implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取消息
        String msgValue = value.toString();
        // 创建partition
        int partition;
        if(msgValue.contains("example")){
            partition = 0;
        }else {
            partition =1;
        }
        return partition;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
