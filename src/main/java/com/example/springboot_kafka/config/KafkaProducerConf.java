package com.example.springboot_kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

// @SpringBootConfiguration
// @Slf4j
public class KafkaProducerConf {

    // @Value("${spring.kafka.producer.bootstrap-servers}")
    // private String bootstrapServers;
    // @Value("${spring.kafka.producer.client-id}")
    // private String clientId;
    // @Value("${spring.kafka.producer.transaction-id-prefix}")
    // private String txIdPrefix;
    // @Value("${spring.kafka.producer.retries}")
    // private String retries;
    // @Value("${spring.kafka.producer.acks}")
    // private String acks;
    // @Value("${spring.kafka.producer.batch-size}")
    // private String batchSize;
    // @Value("${spring.kafka.producer.buffer-memory}")
    // private String bufferMemory;
    // @Value("${spring.kafka.producer.properties.enable.idempotence}")
    // private boolean enableIdempotence;
    // @Value("${spring.kafka.producer.properties.linger.ms}")
    // private Integer lingerMs;
    //
    // @Bean
    // public Map<String, Object> producerConf()
    // {
    //     Map<String, Object> props = new HashMap<>(16);
    //     // 配置broker服务器
    //     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    //     // 客户端ID是用来唯一标识一个 Kafka 生产者的字符串，通常用于日志、监控和识别不同的生产者
    //     props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    //     //acks=0 ： 生产者在成功写入消息之前不会等待任何来自服务器的响应。
    //     //acks=1 ： 只要集群的首领节点收到消息，生产者就会收到一个来自服务器成功响应。
    //     //acks=all ：只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应。
    //     //开启事务必须设为all
    //     props.put(ProducerConfig.ACKS_CONFIG, acks);
    //     //发生错误后，消息重发的次数，开启事务必须大于0
    //     props.put(ProducerConfig.RETRIES_CONFIG, retries);
    //     //当多个消息发送到相同分区时,生产者会将消息打包到一起,以减少请求交互. 而不是一条条发送
    //     //批次的大小可以通过batch.size 参数设置.默认是16KB
    //     //较小的批次大小有可能降低吞吐量（批次大小为0则完全禁用批处理）。
    //     //比如说，kafka里的消息5秒钟Batch才凑满了16KB，才能发送出去。那这些消息的延迟就是5秒钟
    //     //实测batchSize这个参数没有用
    //     props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    //     //有的时刻消息比较少,过了很久,比如5min也没有凑够16KB,这样延时就很大,所以需要一个参数. 再设置一个时间,到了这个时间,
    //     //即使数据没达到16KB,也将这个批次发送出去,单位是ms，变相可以做简单的延时
    //     props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    //     //生产者内存缓冲区的大小
    //     props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    //     //启用生产者的幂等性
    //     props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
    //     //反序列化，和生产者的序列化方式对应
    //     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    //     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    //     // 1.配置自定义的hash分区器类，不配置使用默认的分区策略
    //     // props.put("partitioner.class", "com.example.springboot_kafka.config.KafkaProducerHashPartitioner");
    //     // 2.配置自定义的hash分区器类，不配置使用默认的分区策略
    //     // props.put("partitioner.class", "com.example.springboot_kafka.config.KafkaProducerRandomPartitioner");
    //     return props;
    // }
    //
    // @Bean
    // public ProducerFactory<Object, Object> producerFactory() {
    //     DefaultKafkaProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerConf());
    //     //开启事务，会导致 LINGER_MS_CONFIG 配置失效
    //     producerFactory.setTransactionIdPrefix(txIdPrefix);
    //     // 从连接工厂获取配置信息
    //     // Map<String, Object> producerProps = producerFactory.getConfigurationProperties();
    //     // producerProps.entrySet().forEach(entry -> {
    //     //     log.info("配置项名称：{} ---> {}", entry.getKey(), entry.getValue());
    //     // });
    //     return producerFactory;
    // }
    //
    // @Bean
    // public KafkaTransactionManager<Object, Object> kafkaTransactionManager(ProducerFactory<Object, Object> producerFactory) {
    //     return new KafkaTransactionManager<>(producerFactory);
    // }
    //
    // @Bean
    // public KafkaTemplate<Object, Object> kafkaTemplate() {
    //     return new KafkaTemplate<>(producerFactory());
    // }
}
