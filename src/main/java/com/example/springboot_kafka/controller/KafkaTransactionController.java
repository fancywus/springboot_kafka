package com.example.springboot_kafka.controller;

import cn.hutool.core.date.DateUtil;
import com.example.springboot_kafka.service.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("trans")
@Slf4j
public class KafkaTransactionController {

    @Autowired
    @Qualifier("kafkaProConServiceImpl")
    private KafkaService kafkaService3;

    @GetMapping("send/{msg}")
    public void sendMsg(@PathVariable String msg) {
        log.info("sendMsg控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService3.sendMsg(msg);
    }

    @GetMapping("send/two/{msg}")
    public void sendTwoMsg(@PathVariable String msg) {
        log.info("sendMsg控制层收到请求，请求时间：{}", DateUtil.date());
        kafkaService3.sendTwoTopicMsg(msg);
    }
}
