package com.github.xjtuwsn.cranemq.broker.offset;

import com.alibaba.fastjson.JSON;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.client.ConsumerGroupManager;
import com.github.xjtuwsn.cranemq.common.consumer.ConsumerInfo;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import com.github.xjtuwsn.cranemq.common.utils.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ConsumerOffsetManager
 * @author:wsn
 * @create:2023/10/09-16:47
 */
public class ConsumerOffsetManager {

    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetManager.class);

    // topic@group: [queueId: offset]
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetMap = new ConcurrentHashMap<>();

    private BrokerController brokerController;
    private ConsumerGroupManager consumerGroupManager;

    private String path;

    private File file;

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    // TODO 消费者位移管理，当本来不存在时初始化，然后建立长连接管理服务，
    //  TODO 并定时进行消息拉取，或者存消息时进行通知唤醒，消费者那边消费，消费完更新offset
    public long getOffsetInQueue(String topic, String group, int queueId) {
        String key = BrokerUtil.offsetKey(topic, group);
        if (!offsetMap.containsKey(key)) {
            synchronized (this) {
                if (!offsetMap.containsKey(key)) {
                    ConcurrentHashMap<Integer, Long> map = new ConcurrentHashMap<>();
                    offsetMap.put(key, map);

//                    ConsumerInfo info = consumerGroupManager.getGroupProperity(group);
//                    StartConsume startConsume = info.getStartConsume();
                    StartConsume startConsume = StartConsume.FROM_LAST_OFFSET;
                    int number = brokerController.getMessageStoreCenter().getQueueNumber(topic);
                    for (int i = 0; i < number; i++) {
                        long begin = 0L;
                        if (startConsume == StartConsume.FROM_FIRST_OFFSET) {
                            begin = 0L;
                        } else if (startConsume == StartConsume.FROM_LAST_OFFSET) {
                            begin = brokerController.getMessageStoreCenter().getQueueCurWritePos(topic, i);
                        }
                        if (begin >= 0) {
                            map.put(i, begin);
                        }
                    }
                }
            }

        }
        return offsetMap.get(key).get(queueId);
    }
    public void start() {
        this.path = brokerController.getPersistentConfig().getConsumerOffsetPath();
        File dir = new File(brokerController.getPersistentConfig().getConfigPath());
        if (!dir.exists()) {
            dir.mkdir();
        }
        long start = System.nanoTime();
        this.offsetMap = JSONUtil.loadJSON(path, ConcurrentHashMap.class);
        getOffsetInQueue("Topic1", "ggg", 0);
        getOffsetInQueue("Topic2", "ggg", 0);
        getOffsetInQueue("Topic3", "ggg", 0);
        getOffsetInQueue("Topic4", "ggg", 0);
        getOffsetInQueue("Topic5", "ggg", 0);
        System.out.println(offsetMap);
    }

    public void persistOffset() {
        // JSONUtil.JSONStrToFile(offsetMap, path);
    }
}
