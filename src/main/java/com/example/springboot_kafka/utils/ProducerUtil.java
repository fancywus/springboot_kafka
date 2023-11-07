package com.example.springboot_kafka.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public class ProducerUtil {

    // kafka生产者一
    public static volatile KafkaProducer<Object, Object> kafkaProducer;

    // kafka消费者一
    public static volatile org.apache.kafka.clients.consumer.KafkaConsumer<Object, Object> kafkaConsumer;

    // kafka生产者二
    public static volatile KafkaProducer<Object, Object> twoKafkaProducer;

    // kafka消费者二
    public static volatile org.apache.kafka.clients.consumer.KafkaConsumer<Object, Object> twoKafkaConsumer;

    private static volatile boolean IsSuccess = false;
    private static volatile boolean IsTwoSuccess = false;

    public ProducerUtil() {
    }

    public static KafkaConsumer<Object, Object> getConsumer(KafkaProperties properties) {
        if (kafkaConsumer == null) {
            synchronized (ProducerUtil.class) {
                if (kafkaConsumer == null) {
                    kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties.buildConsumerProperties());
                }
            }
        }
        return kafkaConsumer;
    }

    public static KafkaProducer<Object, Object> getProducer(KafkaProperties properties) {
        if (kafkaProducer == null) {
            synchronized (ProducerUtil.class) {
                if (kafkaProducer == null) {
                    kafkaProducer = new KafkaProducer<>(properties.buildProducerProperties());
                    if (!IsSuccess) {
                        kafkaProducer.initTransactions();
                        System.out.println("*************kafkaProducer只初始化一次****************");
                        IsSuccess = true;
                    }
                }
            }
        }
        return kafkaProducer;
    }

    public static KafkaConsumer<Object, Object> getTwoConsumer(KafkaProperties properties) {
        if (twoKafkaConsumer == null) {
            synchronized (ProducerUtil.class) {
                if (twoKafkaConsumer == null) {
                    twoKafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties.buildConsumerProperties());
                }
            }
        }
        return twoKafkaConsumer;
    }

    public static KafkaProducer<Object, Object> getTwoProducer(KafkaProperties properties) {
        if (twoKafkaProducer == null) {
            synchronized (ProducerUtil.class) {
                if (twoKafkaProducer == null) {
                    twoKafkaProducer = new KafkaProducer<>(properties.buildProducerProperties());
                    if (!IsTwoSuccess) {
                        twoKafkaProducer.initTransactions();
                        System.out.println("*************twoKafkaProducer只初始化一次****************");
                        IsTwoSuccess = true;
                    }
                }
            }
        }
        return twoKafkaProducer;
    }
}
