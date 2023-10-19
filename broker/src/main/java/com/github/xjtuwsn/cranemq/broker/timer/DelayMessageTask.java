package com.github.xjtuwsn.cranemq.broker.timer;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:DelayMessageTask
 * @author:wsn
 * @create:2023/10/19-14:13
 */
public class DelayMessageTask extends DelayTask {

    private static final Logger log = LoggerFactory.getLogger(DelayTaskList.class);

    private String topic;

    private long commitLogOffset;

    private int queueId;
    public DelayMessageTask(BrokerController brokerController) {
        super(brokerController);
    }
    public DelayMessageTask(BrokerController brokerController, String topic, long commitLogOffset, int queueId) {
        this(brokerController);
        this.topic = topic;
        this.commitLogOffset = commitLogOffset;
        this.queueId = queueId;
    }
    // TODO 该task执行将消息写入retry队列，延时后将其对应消息重新写入commitLog，然后正常提交到consumerQueue，需要记录retry次数
    // TODO 在管理程序中，在执行这个延时任务之前，需要先向wheellog持久化相关信息，便于开机恢复
    @Override
    public void run() {

    }

    @Override
    protected int getTaskType() {
        return DELAY_MESSAGE;
    }
}
