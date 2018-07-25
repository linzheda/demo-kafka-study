package com.yc.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author lzd
 * @date 2018/7/3 0003 17:34
 **/
public class KafkaProperties {
    public static Properties getProperties(String name){
        Properties properties=new Properties();
        InputStream resourceAsStream = KafkaProperties.class.getClassLoader().getResourceAsStream(name + ".properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                resourceAsStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }



}
