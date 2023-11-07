package com.example.springboot_kafka.multi.producer.two;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.Map;

@SpringBootConfiguration
@Slf4j
public class TwoKafkaProducerConf {

    @Bean
    public ProducerFactory<Object, Object> twoProducerFactory(@Autowired @Qualifier("twoKafkaProperties")
                                                                  KafkaProperties kafkaProperties) {
        DefaultKafkaProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
        //开启事务，会导致 LINGER_MS_CONFIG 配置失效
        // 如何配置了TransactionIdPrefix和TRANSACTIONAL_ID，将会用TRANSACTIONAL_ID作为前缀覆盖并删除TRANSACTIONAL_ID的这个配置在map中
        // 可以在new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties())这个构造方法看到
        // producerFactory.setTransactionIdPrefix(String.valueOf(kafkaProperties.getProducer().getTransactionIdPrefix()));
        // 从连接工厂获取配置信息
        // Map<String, Object> producerProps = producerFactory.getConfigurationProperties();
        // producerProps.entrySet().forEach(entry -> {
        //     log.info("twoProducerFactory配置项名称：{} ---> {}", entry.getKey(), entry.getValue());
        // });
        return producerFactory;
    }

    @Bean
    public KafkaTransactionManager<Object, Object> twoKafkaTransactionManager(@Autowired @Qualifier("twoProducerFactory")
                                                                                  ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<Object, Object> twoKafkaTemplate(@Autowired @Qualifier("twoProducerFactory")
                                                              ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
