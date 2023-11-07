package com.example.springboot_kafka.listener;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * kafka消息发送回调
 */
@Component("kafkaSendResultHandler")
@Slf4j
public class KafkaSendResultHandler implements ProducerListener<Object, Object> {

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        log.info("监听器：生产者消息已经成功发送到: {}主题，监听器收到时间是：{}", producerRecord.topic(), DateUtil.date());
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata, Exception exception) {
        log.info("监听器：生产者消息发送到 {} 失败，监听器收到时间是：{}", producerRecord.topic(), DateUtil.date());
    }
}
