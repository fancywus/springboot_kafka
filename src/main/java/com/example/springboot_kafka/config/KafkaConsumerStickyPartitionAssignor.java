package com.example.springboot_kafka.config;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * 实现粘性分区使用的抽象类
 */
public class KafkaConsumerStickyPartitionAssignor extends AbstractStickyAssignor {
    @Override
    protected MemberData memberData(Subscription subscription) {
        return null;
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return super.subscriptionUserData(topics);
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        super.onAssignment(assignment, metadata);
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return super.supportedProtocols();
    }

    @Override
    public short version() {
        return super.version();
    }

    @Override
    public String name() {
        return null;
    }
}
