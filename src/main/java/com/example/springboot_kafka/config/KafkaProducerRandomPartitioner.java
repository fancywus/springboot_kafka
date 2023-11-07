package com.example.springboot_kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
 * 自定义随机分区器
 */
@Slf4j
public class KafkaProducerRandomPartitioner implements Partitioner {

   private Random random = new Random();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //1. 根据主题获取到当前的主题的分区数量
        Integer partitionNum = cluster.partitionCountForTopic(topic);
        //2. 随机分区
        int partitionId = random.nextInt(partitionNum);
        log.info("自定义随机分区器计算出来的分区数是: {}", partitionId);
        return partitionId;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
