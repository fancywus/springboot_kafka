package com.example.springboot_kafka.service.impl;

import cn.hutool.core.date.DateUtil;
import com.example.springboot_kafka.listener.KafkaSendResultHandler;
import com.example.springboot_kafka.listener.TwoKafkaSendResultHandler;
import com.example.springboot_kafka.service.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import java.util.HashMap;
import java.util.Map;

@Primary
@Service
@Slf4j
public class KafkaServiceImpl implements KafkaService {

    private static final String TOPIC_NAME = "hello-kafka";

    private static final String TOPIC_NAME_ONE = "hello-kafka";
    private static final String TOPIC_NAME_TWO = "java-kafka";

    private final KafkaTemplate<Object, Object> oneKafkaTemplate;

    private final KafkaTemplate<Object, Object> twoKafkaTemplate;

    private Map<String, Object> map;

    private GenericMessage<Object> message;

    public KafkaServiceImpl(@Autowired
                            @Qualifier("oneKafkaTemplate")
                            KafkaTemplate<Object, Object> oneKafkaTemplate,
                            KafkaSendResultHandler handler,
                            TwoKafkaSendResultHandler twoKafkaSendResultHandler,
                            @Autowired
                            @Qualifier("twoKafkaTemplate")
                            KafkaTemplate<Object, Object> twoKafkaTemplate) {
        this.oneKafkaTemplate = oneKafkaTemplate;
        this.twoKafkaTemplate = twoKafkaTemplate;
        this.oneKafkaTemplate.setProducerListener(handler);
        this.twoKafkaTemplate.setProducerListener(twoKafkaSendResultHandler);
    }

    /**
     * 生产者分区策略：
     * 1.如果指定了partition，按照指定的分区编号发送（手动指定每条消息）
     * 2.如果没有指定partition，但是指定了key，使用key进行hash，根据hash结果选择partition（默认策略：根据key的hash值）
     * 3.如果没有指定partition也没有指定key，那么是轮循的方式选择partition（默认策略：没有key直接轮询）
     * 4.自定义分区策略
     * @param msg
     * @param <T>
     */
    @Override
    //这个注解代表这个类开启Springboot事务，因为我们在Kafka的配置文件开启了Kafka事务，不然会报错
    @Transactional(rollbackFor = Exception.class, transactionManager = "oneKafkaTransactionManager")
    public <T> void sendMsg(T msg) {
        log.info("是否运行在事务中: {}", oneKafkaTemplate.inTransaction());
        try {
            // 生产者发送消息的几种方式
            /**
             * 1.测试批量发送消息
             */
            // for (int i = 0; i < 10; i++) {
            //     kafkaTemplate.send(TOPIC_NAME, msg + "_" + i);
            //     log.info("kafka生产者已经发送第{}条信息到主题: {}，发送时间是：{}", i, TOPIC_NAME, DateUtil.date());
            // }
            /**
             * 2.测试普通发送消息
             */
            // kafkaTemplate.send(TOPIC_NAME, msg);
            /**
             * 3.后面的get代表同步发送，括号内时间可选，代表超过这个时间会抛出超时异常，但是仍会发送成功
             */
            // kafkaTemplate.send(TOPIC_NAME, msg).get(1, TimeUnit.SECONDS);
            /**
             * 4.使用ProducerRecord发送消息
             */
            // ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC_NAME, msg);
            // kafkaTemplate.send(producerRecord);
            /**
             * 5.使用Message发送消息
             */
            // Map<String, Object> msgMap = new HashMap<>();
            // 指定topic名称
            // msgMap.put(KafkaHeaders.TOPIC, TOPIC_NAME);
            // 指定分区id
            // msgMap.put(KafkaHeaders.PARTITION_ID, 0);
            // 指定消息的路由键，类似rabbitmq的routeing-key概念，kafka默认分区策略根据key来投放到某一个分区，不指定key则是轮询方式
            // msgMap.put(KafkaHeaders.MESSAGE_KEY, 0);
            // GenericMessage<Object> genericMessage = new GenericMessage<>(msg, new MessageHeaders(msgMap));
            // kafkaTemplate.send(genericMessage);
            /**
             * 6.测试事务发送消息
             */
            // kafkaTemplate.executeInTransaction(operations -> {
            //     operations.send(TOPIC_NAME,  msg);
            //     // 触发异常，kafka事务回滚，隔离级别读已提交
            //     // int i = 1/ 0;
            //     // 返回 true 提交事务，返回 false 回滚事务
            //     return true;
            // });
            /**
             * 7.测试事务发送多条消息到不同分区
             */
            // for (int i = 0; i < 3; i++) {
            //     Map<String, Object> map = new HashMap<>();
            //     map.put(KafkaHeaders.TOPIC, TOPIC_NAME);
            //     // 1.指定分区
            //     // map.put(KafkaHeaders.PARTITION_ID, 0);
            //     // 2.不去手动指定，使用分区策略来控制，这里使用指定key，看看是否都进入到一个分区
            //     map.put(KafkaHeaders.MESSAGE_KEY, "order");
            //     GenericMessage<Object> message = new GenericMessage<>(msg + "___" + i, new MessageHeaders(map));
            //     kafkaTemplate.executeInTransaction(operations -> {
            //         operations.send(message);
            //         // 返回 true 提交事务，返回 false 回滚事务
            //         return true;
            //     });
            //     log.info("kafka生产者已经发送{}条信息到主题: {}，发送时间是：{}", i, TOPIC_NAME, DateUtil.date());
            // }
            /**
             * 8.测试事务发送多条消息到不同topic的不同分区
             */
            for (int i = 0; i < 3; i++) {
                map = new HashMap<>();
                map.put(KafkaHeaders.TOPIC, TOPIC_NAME_ONE);
                // 1.指定分区
                // map.put(KafkaHeaders.PARTITION_ID, i);
                // 2.不去手动指定，使用分区策略来控制，这里使用指定key，看看是否都进入到一个分区
                // map.put(KafkaHeaders.MESSAGE_KEY, i);
                message = new GenericMessage<>(msg + "___" + i, new MessageHeaders(map));
                oneKafkaTemplate.send(message);
                // executeInTransaction 方法用于在 Spring Kafka 生产者中执行事务。当你使用这个方法时，Spring Kafka 会自动管理事务的开始、提交和回滚。
                // 该方法内部会显式地在方法内部开启和提交事务。因此，每次调用 executeInTransaction 方法时，它都会启动一个新的事务并提交它，所以会有多个生产者实例在同一个事务id中
                // oneKafkaTemplate.executeInTransaction(operations -> {
                //     operations.send(message);
                //     // 返回 true 提交事务，返回 false 回滚事务
                //     return true;
                // });
                log.info("--->第一个kafka生产者已经发送第 {} 条信息到主题: {}，发送时间是：{}", i, TOPIC_NAME_ONE, DateUtil.date());
            }
            // throw new RuntimeException("第一个生产者故意抛出一个运行时异常");
            // log.info("kafka生产者已经发送一条信息到主题: {}，发送时间是：{}", TOPIC_NAME, DateUtil.date());
        }
        catch (Exception e) {
            log.error("--->第一个消息发送异常: {}", e.getMessage());
            // 标记当前事务为回滚状态
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            // 再次抛出异常不进行处理
            // throw e;
        }
    }

    @Override
    //这个注解代表这个类开启Springboot事务，因为我们在Kafka的配置文件开启了Kafka事务，不然会报错
    @Transactional(rollbackFor = Exception.class, transactionManager = "twoKafkaTransactionManager")
    public <T> void sendTwoTopicMsg(T msg) {
        /**
         * 8.测试事务发送多条消息到不同topic的不同分区
         */
        log.info("是否运行在事务中: {}", twoKafkaTemplate.inTransaction());
        try {
            for (int i = 0; i < 3; i++) {
                map = new HashMap<>();
                map.put(KafkaHeaders.TOPIC, TOPIC_NAME_TWO);
                // 1.指定分区
                // map.put(KafkaHeaders.PARTITION_ID, i);
                // 2.不去手动指定，使用分区策略来控制，这里使用指定key，看看是否都进入到一个分区
                // map.put(KafkaHeaders.MESSAGE_KEY, i - 3);
                message = new GenericMessage<>(msg + "---" + i, new MessageHeaders(map));
                twoKafkaTemplate.send(message);
                // executeInTransaction 方法用于在 Spring Kafka 生产者中执行事务。当你使用这个方法时，Spring Kafka 会自动管理事务的开始、提交和回滚。
                // 该方法内部会显式地在方法内部开启和提交事务。因此，每次调用 executeInTransaction 方法时，它都会启动一个新的事务并提交它，所以会有多个生产者实例在同一个事务id中
                // twoKafkaTemplate.executeInTransaction(operations -> {
                //     operations.send(message);
                //     // 返回 true 提交事务，返回 false 回滚事务
                //     return true;
                // });
                log.info("***>第二个kafka生产者已经发送第 {} 条信息到主题: {}，发送时间是：{}", i, TOPIC_NAME_TWO, DateUtil.date());
            }
            // throw new RuntimeException("第二个生产者故意抛出一个运行时异常");
        }
        catch (Exception e) {
            log.error("***>第二个消息发送异常: {}", e.getMessage());
            // 标记当前事务为回滚状态
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            // 再次抛出异常不进行处理
            // throw e;
        }
    }
}
