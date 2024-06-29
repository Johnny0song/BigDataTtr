package com.example.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomProducerCallback {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建kafka生产者的配置对象
        Properties properties = new Properties();

        // 2.给kafka配置对象添加配置信息 bootstrap。servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");

        // 3.key,value 序列化（必须）：key.serializer , value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // 4.调用send方法，发送消息
        for(int i=0; i < 5; i++){
            kafkaProducer.send(new ProducerRecord<String, String>("first", "example " + i), new Callback() {
                // 改方法在Producer收到ack时调用，为异步调用
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println("主题：" + metadata.topic() + "-> " + "分区：" + metadata.partition());
                    }else {
                        // 出现异常
                        exception.printStackTrace();
                    }
                }

            });
            // 延迟一会看到数据发往不同分区
            Thread.sleep(2);
        }

        // 5.关闭资源
        kafkaProducer.close();

    }
}
