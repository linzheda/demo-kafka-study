package com.yc.kafka.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author lzd
 * @date 2018/7/4 0004 10:52
 **/
public class MyPartition implements Partitioner {

    /**
     *
     * @param topic 主题
     * @param key 键
     * @param keyBytes 键的字节数组
     * @param value
     * @param valueBytes
     * @param cluster 集群信息
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //如果分区数不到3可以进入0
        Integer count=cluster.partitionCountForTopic(topic);
        String keyString=key.toString();
        //判断分区个数和key的值是否为空
        if(count==3&&keyString!=null){
           // 通过不同的key 来确定进入哪个分区
            if (keyString.startsWith("135")){
                return 0;
            }else if(keyString.startsWith("130")){
                return 1;
            }else if(keyString.startsWith("180")){
                return 2;
            }
        }
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
