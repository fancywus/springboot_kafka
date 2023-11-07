package com.example.springboot_kafka.service.impl;

import com.example.springboot_kafka.service.KafkaService;
import com.example.springboot_kafka.utils.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * 直接使用kafka提供的生产者事务方法，单纯的生产者事务模式，只有写，无法保证消费者精确消费
 */
@Service
@Slf4j
public class KafkaServiceTransImpl implements KafkaService {

    private static final String TOPIC_NAME_ONE = "hello-kafka";
    private static final String TOPIC_NAME_TWO = "java-kafka";
    public final KafkaProperties threeKafkaProperties;
    public final KafkaProperties fourKafkaProperties;

    public KafkaServiceTransImpl(@Autowired
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
        KafkaProducer<Object, Object> oneProducer = ProducerUtil.getProducer(threeKafkaProperties);
        // 1.初始化事务
        // oneProducer.initTransactions();
        try {
            // 2.开启事务
            oneProducer.beginTransaction();
            for (int i = 0; i < 3; i++) {
                ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC_NAME_ONE, msg + "_" + i);
                oneProducer.send(producerRecord);

            }
            // kafka事务是类似分布式事务两阶段过程,异常触发必须要在提交事务之前，否则将无法中止事务
            // int i = 1 / 0;
            // 3.提交事务
            oneProducer.commitTransaction();
        }
        catch (Exception e) {
            log.error("第一个生产者报告异常： {}", e.getMessage());
            // 4.放弃事务（类似于回滚事务的操作）
            oneProducer.abortTransaction();
        }
        // finally {
        //     if (oneProducer != null) {
        //         // 5.关闭生产者
        //         oneProducer.close();
        //     }
        // }
    }

    @Override
    public <T> void sendTwoTopicMsg(T msg) {
        // 1.初始化事务
        // twoProducer.initTransactions();
        KafkaProducer<Object, Object> twoProducer = ProducerUtil.getTwoProducer(fourKafkaProperties);
        try {
            // 2.开启事务
            // twoProducer.beginTransaction();
            for (int i = 0; i < 3; i++) {
                ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC_NAME_TWO, msg + "_" + i);
                twoProducer.send(producerRecord);
            }
            // kafka事务是类似分布式事务两阶段过程,异常触发必须要在提交事务之前，否则将无法中止事务
            // int i = 1 / 0;
            // 3.提交事务
            twoProducer.commitTransaction();
        }
        catch (Exception e) {
            log.error("第二个生产者报告异常： {}", e.getMessage());
            // 4.放弃事务（类似于回滚事务的操作）
            twoProducer.abortTransaction();
        }
        // finally {
        //     if (twoProducer != null) {
        //         // 5.关闭生产者
        //         twoProducer.close();
        //     }
        // }
    }


}
