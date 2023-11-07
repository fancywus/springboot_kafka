package com.example.springboot_kafka.topic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * topic基本操作
 */
public class TopicTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Map<String, Object> map = new HashMap<>();
        map.put("bootstrap.servers", "39.108.57.96:9092,121.37.238.132:9092,121.37.238.132:9093");
        // 创建管理员对象
        AdminClient client = AdminClient.create(map);
        // 1.通过管理员对象创建主题
        client.createTopics(Arrays.asList(new NewTopic("java-kafka", 3, (short) 2)));
        // 2.查询全部主题
        ListTopicsResult listedTopics = client.listTopics();
        KafkaFuture<Set<String>> names = listedTopics.names();
        Set<String> topics = names.get();
        for (String topic : topics) {
            System.out.println("主题名称: " + topic);
            // 3.查询主题详情
            DescribeTopicsResult describeTopics = client.describeTopics(Arrays.asList(topic));
            KafkaFuture<Map<String, TopicDescription>> topicDescs = describeTopics.allTopicNames();
            Map<String, TopicDescription> descriptionMap = topicDescs.get();
            for (Map.Entry<String, TopicDescription> entry : descriptionMap.entrySet()) {
                System.out.println("------------" + entry.getKey() + "----------");
                TopicDescription value = entry.getValue();
                List<TopicPartitionInfo> partitions = value.partitions();
                for (TopicPartitionInfo partition : partitions) {
                    System.out.println(partition.partition() + "--->" + "Leader: " + partition.leader() + "replicas: " + partition.replicas());
                }
            }
        }
        // 释放资源
        client.close();
    }
}
