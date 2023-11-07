package com.example.springboot_kafka.multi.consumer.two;

import com.example.springboot_kafka.annotation.ConditionalOnMultiKafkaConsumerEnabled;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * 配置第二个消费者
 */
@SpringBootConfiguration
@Slf4j
// @ConditionalOnMultiKafkaConsumerEnabled
public class TwoKafkaConsumerConf {

    @Bean
    public KafkaListenerContainerFactory twoKafkaListenerContainerFactory(@Autowired @Qualifier("twoKafkaProperties") KafkaProperties kafkaProperties,
                                                                          @Autowired @Qualifier("twoConsumerFactory") ConsumerFactory consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> f = new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(consumerFactory);
        //在侦听器容器中运行的线程数，一般设置为 机器数*分区数
        f.setConcurrency(kafkaProperties.getListener().getConcurrency());
        //消费监听接口监听的主题不存在时，默认会报错，所以设置为false忽略错误
        f.setMissingTopicsFatal(kafkaProperties.getListener().isMissingTopicsFatal());
        //自动提交关闭，需要设置手动消息确认
        f.getContainerProperties().setAckMode(kafkaProperties.getListener().getAckMode());
        f.getContainerProperties().setPollTimeout(kafkaProperties.getListener().getPollTimeout().toMillis());
        //设置为批量监听，需要用List接收批量消息
        // f.setBatchListener(true);
        return f;
    }

    @Bean
    public ConsumerFactory twoConsumerFactory(@Autowired @Qualifier("twoKafkaProperties") KafkaProperties kafkaProperties) {
        //配置消费者的 Json 反序列化的可信赖包，反序列化实体类需要
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>();
        // 不允许同时使用两者否则报错, 配置 Kafka 消费者时，你尝试同时使用 JsonDeserializer 的属性设置器("*")和配置属性(yml)
        // String packages = kafkaProperties.getConsumer().getProperties().get("spring.json.trusted.packages");
        deserializer.addTrustedPackages("*");
        return new DefaultKafkaConsumerFactory(kafkaProperties.buildConsumerProperties(), new JsonDeserializer<>(), deserializer);
    }

    @Bean
    @ConfigurationProperties(prefix = "fancywu.kafka.two")
    public KafkaProperties twoKafkaProperties(){
        return new KafkaProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "fancywu.kafka.four")
    public KafkaProperties fourKafkaProperties(){
        return new KafkaProperties();
    }
}
