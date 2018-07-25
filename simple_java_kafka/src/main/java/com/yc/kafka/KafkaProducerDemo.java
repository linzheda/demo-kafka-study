package com.yc.kafka;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 生产者
 *
 * @Author: 林哲达
 * @Date: 2018/3/22 21:23
 */
public class KafkaProducerDemo {
    private final Producer<String, String> kafkaProducer;

    public final static String TOPIC = "JAVA_TOPIC";

    private KafkaProducerDemo() {
        kafkaProducer = createKafkaProducer();
    }

    private Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        //服务器ip:端口号，集群用逗号分隔
        props.put("bootstrap.servers", "192.168.1.134:9092");
        //props.put("bootstrap.servers", "192.168.1.128:9092,192.168.1.129:9092,192.168.1.130:9092,192.168.1.131:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
        return kafkaProducer;
    }

    void produce() {
        for (int i = 1; i < 20; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String key = String.valueOf("key" + i);
            String data = "hello kafka message:" + key;
            kafkaProducer.send(new ProducerRecord<>(TOPIC, key, data), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("the producer has a error:" + e.getMessage());
                    } else {
                        System.out.println("The offset of the record we just sent is: "+ recordMetadata.offset());
                        System.out.println("The partition of the record we just sent is: "+ recordMetadata.partition());
                    }
                }
            });
            System.out.println(data);
        }
    }

    public static void main(String[] args) {
        new KafkaProducerDemo().produce();
    }
}
