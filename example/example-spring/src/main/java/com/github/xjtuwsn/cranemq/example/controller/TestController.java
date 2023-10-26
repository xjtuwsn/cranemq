package com.github.xjtuwsn.cranemq.example.controller;

import com.github.xjtuwsn.cranemq.example.service.MessageProducer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:TestController
 * @author:wsn
 * @create:2023/10/25-22:23
 */
@RestController
public class TestController {

    @Resource
    public MessageProducer messageProducer;
    @RequestMapping("/hello")
    public String hello() {
        messageProducer.sendMessageSimple();
        return "done";
    }
    @RequestMapping("/ordered")
    public String ordered() {
        messageProducer.sendMessageOrder();
        return "done";
    }
    @RequestMapping("/delay/{second}")
    public String delay(@PathVariable("second") String second) {
        messageProducer.sendMessageDelay(Integer.parseInt(second), TimeUnit.SECONDS);
        return "done";
    }
}
