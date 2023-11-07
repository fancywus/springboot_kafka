package com.example.springboot_kafka.service;

public interface KafkaService {

    <T> void sendMsg(T msg);

    <T> void sendTwoTopicMsg(T msg);
}
