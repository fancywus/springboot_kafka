package com.example.springboot_kafka.service.impl;

import com.example.springboot_kafka.service.KafkaService;
import com.example.springboot_kafka.utils.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 实现消费-生产并存模式，即在一个事务内,即有生产消息又有消费消息，即常说的Consume-tansform-produce模式
 */
@Service
@Slf4j
public class KafkaProConServiceImpl implements KafkaService {

    private static final String TOPIC_NAME_ONE = "hello-kafka";
    private static final String TOPIC_NAME_TWO = "java-kafka";

    public final KafkaProperties threeKafkaProperties;
    public final KafkaProperties fourKafkaProperties;

    public KafkaProConServiceImpl(@Autowired
                                  @Qualifier("threeKafkaProperties")
                                  KafkaProperties threeKafkaProperties,
                                  @Autowired
                                  @Qualifier("fourKafkaProperties")
                                  KafkaProperties fourKafkaProperties) {
        this.threeKafkaProperties = threeKafkaProperties;
        this.fourKafkaProperties = fourKafkaProperties;
    }

    @Override
    public <T> void sendMsg(T msg) {
        KafkaProducer<Object, Object> producer = ProducerUtil.getProducer(threeKafkaProperties);
        KafkaConsumer<Object, Object> consumer = ProducerUtil.getConsumer(threeKafkaProperties);
        // 消费者订阅主题或者指定分区
        consumer.subscribe(Arrays.asList(TOPIC_NAME_ONE));
        boolean isPoll = true;
        while (isPoll) {
            // 生产者开启事务
            try {
                isPoll = false;

                producer.beginTransaction();

                // 让生产者生产消息
                for (int i = 0; i < 6; i++) {
                    producer.send(new ProducerRecord<>(TOPIC_NAME_ONE, String.valueOf(i), msg + "_" + i));
                    log.info("生产者发送第 {} 消息.....", i);
                }

                // 从Kafka服务器获取消息的方法，传入参数表示n个毫秒时间单位，将在这个超时前将会得到返回消息记录，如果没有记录是null
                ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofMillis(3000));
                log.info("每次消费者获得的消息记录是否为空: {}", consumerRecords.isEmpty());
                // 用于存放提交的偏移量，手动提交
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                    // 读取消息操作
                    log.info("在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}",
                            consumerRecord.topic(), consumerRecord.partition(),
                            consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                    // 记录提交的主题分区的偏移量
                    commits.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset()));

                }

                // 生产者提交偏移量
                producer.sendOffsetsToTransaction(commits, new ConsumerGroupMetadata("C"));

                // 生产者提交事务
                producer.commitTransaction();
            }
            catch (Exception e) {
                log.error("发送消息产生异常: {}", e.getMessage());
                // 生产者回滚事务
                producer.abortTransaction();
            }
        }
    }

    @Override
    public <T> void sendTwoTopicMsg(T msg) {
        KafkaProducer<Object, Object> producer = ProducerUtil.getTwoProducer(fourKafkaProperties);
        KafkaConsumer<Object, Object> consumer = ProducerUtil.getTwoConsumer(fourKafkaProperties);
        // 消费者订阅主题或者指定分区
        consumer.subscribe(Arrays.asList(TOPIC_NAME_TWO));
        // 消费者发生崩溃或者有新的消费者加入消费者组，就会触发再均衡Rebalance，Rebalance之后，每个消费者将会分配到新的分区
        // 消费数据处理业务完成后进行offset提交，可以保证数据最少一次消费，因为在提交offset的过程中可能出现提交失败的情况，导致数据重复消费
        // consumer.subscribe(Arrays.asList(TOPIC_NAME_TWO), new ConsumerRebalanceListener() {
        //
        //     //重平衡之前执行
        //     @Override
        //     public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        //         // 确保在重平衡的时候也能提交成功
        //         consumer.commitAsync();
        //     }
        //
        //     //重新分配分区时执行
        //     @Override
        //     public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        //
        //     }
        // });
        boolean isPoll = true;
        while (isPoll) {
            // 生产者开启事务
            try {

                isPoll = false;

                producer.beginTransaction();

                // 让生产者生产消息
                for (int i = 0; i < 6; i++) {
                    producer.send(new ProducerRecord<>(TOPIC_NAME_TWO, String.valueOf(i), msg + "_" + i));
                    log.info("生产者发送第 {} 消息.....", i);
                }

                // 从Kafka服务器获取消息的方法，传入参数表示n个毫秒时间单位，将在这个超时前将会得到返回消息记录，如果没有记录是null
                ConsumerRecords<Object, Object> consumerRecords = consumer.poll(3000);
                log.info("每次消费者获得的消息记录是否为空: {}", consumerRecords.isEmpty());
                // 用于存放提交的偏移量，手动提交
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                TopicPartition topicPartition = null;
                for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                    // 读取消息操作
                    log.info("在 {} 主题(Topic)的 {} 分区(Partition)中,  偏移量(offset): {}, 消息键(key): {}, 消息值(value): {}",
                            consumerRecord.topic(), consumerRecord.partition(),
                            consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                    // 记录提交的主题分区的偏移量
                    topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset());
                    commits.put(topicPartition, offsetAndMetadata);
                }

                // 消费者同步提交方法
                // 这是一个同步方法，它将当前消费者组的偏移量提交到 Kafka 服务器，并等待确认。这意味着它会阻塞当前线程，直到提交成功或失败。
                // consumer.commitSync();

                // 消费者异步提交方法
                // 这是一个异步方法，它会将偏移量提交请求发送给 Kafka 服务器，但不会等待确认。它允许你在提交偏移量的同时继续处理消息，不会阻塞当前线程。
                // consumer.commitAsync(commits, new OffsetCommitCallback() {
                //     @Override
                //     public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                //          if (exception == null) {
                //             // 提交成功的处理逻辑
                //             //  如果发生异常,尝试重试
                //              consumer.commitAsync(offsets);
                //         } else {
                //             // 提交失败的处理逻辑
                //         }
                //     }
                // });

                // 生产者提交偏移量
                producer.sendOffsetsToTransaction(commits, new ConsumerGroupMetadata("D"));

                // 如果最后一次不等于空，就将分区偏移量提交，就是最高水位线
                if (topicPartition != null) {
                    consumer.seekToEnd(Arrays.asList(topicPartition));
                }
                // 生产者提交事务
                producer.commitTransaction();
            }
            catch (Exception e) {
                log.error("发送消息产生异常: {}", e.getMessage());
                // 生产者回滚事务
                producer.abortTransaction();
            }
            // finally {
            //     //在consumer关闭之前进行同步提交,保证所有offset在程序退出之前提交一次
            //     consumer.commitSync();
            //     consumer.close();
            // }
        }
    }
}
