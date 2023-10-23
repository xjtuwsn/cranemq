package com.github.xjtuwsn.cranemq.broker.web.controller;

import com.github.xjtuwsn.cranemq.common.entity.QueueInfo;
import com.github.xjtuwsn.cranemq.broker.web.service.QueueService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:QueueController
 * @author:wsn
 * @create:2023/10/23-18:59
 */
@RestController
@RequestMapping("/queue")
public class QueueController {

    @Resource
    private QueueService queueService;

    @RequestMapping("/list")
    public Map<String, List<QueueInfo>> listQueues() {
        return queueService.listQueues();
    }
}
