package com.example.springboot_kafka.config;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * kafka实现自定义消费者分配分区策略接口实现类
 */
public class KafkaConsumerPartitionAssignor implements ConsumerPartitionAssignor {

    /**
     * 返回序列化后的自定义数据
     */
    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return ConsumerPartitionAssignor.super.subscriptionUserData(topics);
    }

    /**
     * 分区分配的计算逻辑
     */
    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription groupSubscription) {
        return null;
    }

    /**
     * 当组成员从领导者那里收到其分配时调用的回调
     */
    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        ConsumerPartitionAssignor.super.onAssignment(assignment, metadata);
    }

    /**
     * 指明使用的再平衡协议
     * 默认使用ReBalanceProtocol.EAGER协议, 另外一个可选项为 ReBalanceProtocol.COOPERATIVE
     */
    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return ConsumerPartitionAssignor.super.supportedProtocols();
    }

    /**
     * Return the version of the assignor which indicates how the user metadata encodings
     * and the assignment algorithm gets evolved.
     */
    @Override
    public short version() {
        return ConsumerPartitionAssignor.super.version();
    }

    /**
     * 分配器的名字
     * 例如 RangeAssignor、RoundRobinAssignor、StickyAssignor、CooperativeStickyAssignor
     * 对应的名字为
     * range、roundRobin、sticky、cooperative-sticky
     */
    @Override
    public String name() {
        return null;
    }
}
