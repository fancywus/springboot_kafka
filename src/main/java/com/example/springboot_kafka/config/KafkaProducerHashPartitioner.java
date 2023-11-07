package com.example.springboot_kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义Hash分区器
 */
@Slf4j
public class KafkaProducerHashPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer countForTopic = cluster.partitionCountForTopic(topic);
        int partitionId = 0;
        if (key != null) {
            partitionId = Math.abs(key.hashCode() % countForTopic);
        }
        log.info("自定义Hash分区器计算出来的分区数是: {}", partitionId);
        return partitionId;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
