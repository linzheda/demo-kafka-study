package com.yc.kafka;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者
 *
 * @Author: 林哲达
 * @Date: 2018/3/22 21:26
 */
public class KafkaConsumerDemo {

    private final KafkaConsumer<String, String> consumer;

    private KafkaConsumerDemo() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.134:9092");
        //props.put("bootstrap.servers", "192.168.1.128:9092,192.168.1.129:9092,192.168.1.130:9092,192.168.1.131:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    void consume() {
        consumer.subscribe(Arrays.asList(KafkaProducerDemo.TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    public static void main(String[] args) {
        new KafkaConsumerDemo().consume();
    }
}
