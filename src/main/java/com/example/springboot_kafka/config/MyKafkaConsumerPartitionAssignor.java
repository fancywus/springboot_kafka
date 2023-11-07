package com.example.springboot_kafka.config;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 自定义实现的随机选择消费者分配器
 */
public class MyKafkaConsumerPartitionAssignor extends AbstractPartitionAssignor {
    private Map<String, List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(res, topic, consumerId);
            }
        }
        return res;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 得到topic和订阅该topic的消费者集合(参考RangeAssignor中的consumersPerTopic()方法)
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }

        // 遍历每个topic
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            // 订阅当前topic的所有消费者集合
            List<String> consumersForTopic = topicEntry.getValue();
            int consumerSize = consumersForTopic.size();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null) {
                continue;
            }

            // 当前topic下所有分区
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            for (TopicPartition partition:partitions){
                // 随机选择一个消费者
                int rand = new Random().nextInt(consumerSize);
                // 得到随机选择的消费者
                String selectedConsumer = consumersForTopic.get(rand);
                // 给选择的消费者分配当前分区
                assignment.get(selectedConsumer).add(partition);
            }
        }
        System.out.println("分配结果: " + JSONUtil.parse(assignment));
        return assignment;
    }


    @Override
    public String name() {
        return "MyKafkaConsumerPartitionAssignor";
    }
}
