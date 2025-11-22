package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProduceDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);




        for (int i = 0; i < 50; i++) {
             producer.send(new ProducerRecord("second", "value-" + i), new Callback() {
                 @Override
                 public void onCompletion(RecordMetadata metadata, Exception exception) {
                     System.out.println("Topic:" + metadata.topic() + "\t" +
                             "Partition:" + metadata.partition() + "\t" +
                             "Offset:" + metadata.offset());
                 }
             });
//            producer.send(new ProducerRecord("second", "value-" + i));

            Thread.sleep(10);

        }

        producer.close();
    }
}
