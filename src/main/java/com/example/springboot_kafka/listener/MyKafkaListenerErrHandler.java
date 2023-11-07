package com.example.springboot_kafka.listener;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * 监听到异常处理
 */
@Component
@Slf4j
public class MyKafkaListenerErrHandler implements KafkaListenerErrorHandler {
    @Override
    @NonNull
    public Object handleError(@NonNull Message<?> message, @NonNull ListenerExecutionFailedException e) {
        return new Object();
    }

    @Override
    @NonNull
    public Object handleError(@NonNull Message<?> message, @NonNull ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        log.error("消息详情：{}",message);
        log.error("异常信息：{}", exception.getMessage());
        log.error("消费者详情：{}", consumer.groupMetadata());
        log.error("监听主题: {}", consumer.listTopics());
        return KafkaListenerErrorHandler.super.handleError(message, exception, consumer);
    }
}
