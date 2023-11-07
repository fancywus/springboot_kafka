package com.example.springboot_kafka.annotation;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.lang.annotation.*;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ConditionalOnProperty(prefix = "fancywu.kafka.multi", name = "enable", havingValue = "true")
public @interface ConditionalOnMultiKafkaConsumerEnabled {

}
