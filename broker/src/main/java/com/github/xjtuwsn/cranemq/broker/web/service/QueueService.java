package com.github.xjtuwsn.cranemq.broker.web.service;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.entity.QueueInfo;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:QueueService
 * @author:wsn
 * @create:2023/10/23-19:00
 */
@Service
@DependsOn(value = "brokerController")
public class QueueService {
    @Resource
    private BrokerController brokerController;

    public Map<String, List<QueueInfo>> listQueues() {
        return brokerController.getMessageStoreCenter().getAllQueueInfos();
    }
}
