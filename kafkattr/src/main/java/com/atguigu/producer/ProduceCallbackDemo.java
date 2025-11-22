package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProduceCallbackDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefPartitioner.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        List<String> listWord = Arrays.asList("atguigu", "hello", "world", "kafka", "java", "scala", "python", "bigdata", "hadoop", "spark");


        for (int i = 0; i < 10; i++) {
            DefPartitioner defPartitioner = new DefPartitioner();
            producer.send(new ProducerRecord("first","key-"+i ,listWord.get(i)),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                System.out.println("Topic:" + metadata.topic() + "\t" +
                                        "Partition:" + metadata.partition() + "\t" +
                                        "Offset:" + metadata.offset());
                            } else {
                                exception.printStackTrace();
                            }
                        }
                    });
        }

        producer.close();
    }
}
