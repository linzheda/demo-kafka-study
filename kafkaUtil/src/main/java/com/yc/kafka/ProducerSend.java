package com.yc.kafka;

import com.yc.kafka.util.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author lzd
 * @date 2018/7/3 0003 9:46
 **/
public class ProducerSend {
    public static void main(String[] args) {
        Producer<String, String> producer = new KafkaProducer<>(KafkaProperties.getProperties("producer"));
        for (int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }
}
