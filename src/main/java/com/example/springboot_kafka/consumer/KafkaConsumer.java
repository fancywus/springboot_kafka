package com.example.springboot_kafka.consumer;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * kafka监听消息
 */
@Component
@Slf4j
public class KafkaConsumer {

    private static final String TOPIC_NAME_ONE = "hello-kafka";
    private static final String TOPIC_NAME_TWO = "java-kafka";

    //************************************** 消费组的单个消费者消费多个分区 *********************************************

    /**
     * 这样相当于这个消费者组消费者监听整个topic所有数据
     * 每次监听kafka消息，同时配置了错误处理器
     * @param record  kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    // @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = TOPIC_NAME, errorHandler = "myKafkaListenerErrHandler")
    // public void listenerTopic(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
    //     try {
    //         // 触发异常，myKafkaListenerErrHandler错误处理器负责处理
    //         // int i = 1 / 0;
    //         log.info("消费者已经成功获取到消息：{}, 收到时间是：{}", record.value(), DateUtil.date());
    //     } finally {
    //         // 手动确认
    //         ack.acknowledge();
    //     }
    // }

    //************************************** 消费者组A的多个消费者消费多个分区 *********************************************

    // /**
    //  * 消费组A中第一个消费者listenerTopicConsumer1消费分区0的消息
    //  * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
    //  * @param ack kafka的消息确认
    //  */
    // @KafkaListener(id = "001", groupId = "${spring.kafka.consumer.group-id}",
    //         errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
    //                 @TopicPartition(topic = TOPIC_NAME, partitions = {"0"})
    // })
    // public void listenerTopicConsumer1(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
    //     try {
    //         log.info("listenerTopicConsumer1-已经成功获取到消息：{}, 收到时间是：{}", record.value(), DateUtil.date());
    //     } finally {
    //         // 手动确认
    //         ack.acknowledge();
    //     }
    // }
    //
    // /**
    //  * 消费组A中第二个消费者listenerTopicConsumer2消费分区1的消息
    //  * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
    //  * @param ack kafka的消息确认
    //  */
    // @KafkaListener(id = "002", groupId = "${spring.kafka.consumer.group-id}",
    //         errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
    //                 @TopicPartition(topic = TOPIC_NAME, partitions = {"1"})
    //         })
    // public void listenerTopicConsumer2(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
    //     try {
    //         log.info("listenerTopicConsumer2-已经成功获取到消息：{}, 收到时间是：{}", record.value(), DateUtil.date());
    //     } finally {
    //         // 手动确认
    //         ack.acknowledge();
    //     }
    // }
    //
    // /**
    //  * 消费组A中第三个消费者listenerTopicConsumer3消费分区2的消息
    //  * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
    //  * @param ack kafka的消息确认
    //  */
    // @KafkaListener(id = "003", groupId = "${spring.kafka.consumer.group-id}",
    //         errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
    //                 @TopicPartition(topic = TOPIC_NAME, partitions = {"2"})
    //         })
    // public void listenerTopicConsumer3(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
    //     try {
    //         log.info("listenerTopicConsumer3-已经成功获取到消息：{}, 收到时间是：{}", record.value(), DateUtil.date());
    //     } finally {
    //         // 手动确认
    //         ack.acknowledge();
    //     }
    // }

    //************************************** 消费者组B的多个消费者消费多个分区 *********************************************

    /**
     * 消费组B中第一个消费者listenerTopicConsumer1消费分区0的消息
     * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    @KafkaListener(id = "B-001", groupId = "${fancywu.kafka.one.consumer.group-id}",
            errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME_ONE, partitions = {"0"})},
            containerFactory = "oneKafkaListenerContainerFactory"
    )
    public void listenerTopicBConsumer1(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            log.info("消费组B第一个消费者-listenerTopicBConsumer1-获取在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}, 收到时间是：{}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    record.value(), DateUtil.date());
        } finally {
            // 手动确认
            ack.acknowledge();
        }
    }

    /**
     * 消费组B中第二个消费者listenerTopicConsumer2消费分区1的消息
     * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    @KafkaListener(id = "B-002", groupId = "${fancywu.kafka.one.consumer.group-id}",
            errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME_ONE, partitions = {"1"})},
            containerFactory = "oneKafkaListenerContainerFactory"
    )
    public void listenerTopicBConsumer2(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            log.info("消费组B第二个消费者-listenerTopicBConsumer2-获取在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}, 收到时间是：{}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    record.value(), DateUtil.date());
        } finally {
            // 手动确认
            ack.acknowledge();
        }
    }

    /**
     * 消费组B中第三个消费者listenerTopicConsumer3消费分区2的消息
     * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    @KafkaListener(id = "B-003", groupId = "${fancywu.kafka.one.consumer.group-id}",
            errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME_ONE, partitions = {"2"})},
            containerFactory = "oneKafkaListenerContainerFactory"
    )
    public void listenerTopicBConsumer3(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            log.info("消费组B第三个消费者-listenerTopicBConsumer3-获取在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}, 收到时间是：{}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    record.value(), DateUtil.date());
        } finally {
            // 手动确认
            ack.acknowledge();
        }
    }

    //************************************** 消费者组C的多个消费者消费多个分区 *********************************************

    /**
     * 消费组C中第一个消费者listenerTopicConsumer1消费分区0的消息
     * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    @KafkaListener(id = "C-001", groupId = "${fancywu.kafka.two.consumer.group-id}",
            errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME_TWO, partitions = {"0"})},
            containerFactory = "twoKafkaListenerContainerFactory"
    )
    public void listenerTopicCConsumer1(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            log.info("消费组C第一个消费者-listenerTopicCConsumer1-获取在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}, 收到时间是：{}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    record.value(), DateUtil.date());
        } finally {
            // 手动确认
            ack.acknowledge();
        }
    }

    /**
     * 消费组C中第二个消费者listenerTopicConsumer2消费分区1的消息
     * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    @KafkaListener(id = "C-002", groupId = "${fancywu.kafka.two.consumer.group-id}",
            errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME_TWO, partitions = {"1"})},
            containerFactory = "twoKafkaListenerContainerFactory"
    )
    public void listenerTopicCConsumer2(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            log.info("消费组C第二个消费者-listenerTopicCConsumer2-获取在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}, 收到时间是：{}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    record.value(), DateUtil.date());
        } finally {
            // 手动确认
            ack.acknowledge();
        }
    }

    /**
     * 消费组C中第三个消费者listenerTopicConsumer3消费分区2的消息
     * @param record Kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
     * @param ack kafka的消息确认
     */
    @KafkaListener(id = "C-003", groupId = "${fancywu.kafka.two.consumer.group-id}",
            errorHandler = "myKafkaListenerErrHandler", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME_TWO, partitions = {"2"})},
            containerFactory = "twoKafkaListenerContainerFactory"
    )
    public void listenerTopicCConsumer3(ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            log.info("消费组C第三个消费者-listenerTopicCConsumer3-获取在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}, 收到时间是：{}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    record.value(), DateUtil.date());
        } finally {
            // 手动确认
            ack.acknowledge();
        }
    }

    //************************************** 单个消费组的消费者批量消费 *********************************************

    // /**
    //  * 监听批量kafka消息，同时配置了错误处理器
    //  * @param record  kafka的消息，用consumerRecord可以接收到更详细的信息，也可以用String message只接收消息
    //  * @param ack kafka的消息确认
    //  */
    // @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = TOPIC_NAME, errorHandler = "myKafkaListenerErrHandler")
    // public void listenerListTopic(List<ConsumerRecord<Object, Object>> record, Acknowledgment ack) {
    //     try {
    //         // 触发异常，myKafkaListenerErrHandler错误处理器负责处理
    //         // int i = 1 / 0;
    //         log.info("获取到批量消息有{}条!", record.size());
    //         for (int i = 0; i < record.size(); i++) {
    //             log.info("消费者已经成功获取到消息：{}, 收到时间是：{}", record.get(i).value(), DateUtil.date());
    //         }
    //     } finally {
    //         // 手动确认
    //         ack.acknowledge();
    //     }
    // }
}
