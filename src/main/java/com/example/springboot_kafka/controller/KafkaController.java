package com.example.springboot_kafka.controller;

import cn.hutool.core.date.DateUtil;
import com.example.springboot_kafka.pojo.User;
import com.example.springboot_kafka.service.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

@RestController
@RequestMapping("kafka")
@Slf4j
public class KafkaController {

    @Autowired
    @Qualifier("kafkaServiceImpl")
    private KafkaService kafkaService;

    @Autowired
    @Qualifier("kafkaServiceTransImpl")
    private KafkaService kafkaService2;

    @GetMapping("send/{msg}")
    public void sendMsg(@PathVariable String msg) {
        log.info("sendMsg控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService.sendMsg(msg);
    }

    @GetMapping("send/two/{msg}")
    public void sendTwoMsg(@PathVariable String msg) {
        log.info("sendTwoMsg控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService.sendTwoTopicMsg(msg);
    }

    @PostMapping("sendObj")
    public void sendObj(@RequestBody User user) {
        log.info("控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService.sendMsg(user);
    }

    @GetMapping("send/trans/{msg}")
    public void sendTransMsg(@PathVariable String msg) {
        log.info("sendTransMsg控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService2.sendMsg(msg);
    }

    @GetMapping("send/trans/two/{msg}")
    public void sendTransTwoMsg(@PathVariable String msg) {
        log.info("sendTransTwoMsg控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService2.sendTwoTopicMsg(msg);
    }
}
