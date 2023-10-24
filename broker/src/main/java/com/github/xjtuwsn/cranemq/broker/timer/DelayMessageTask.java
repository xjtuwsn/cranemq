package com.github.xjtuwsn.cranemq.broker.timer;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreInnerMessage;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
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

/**
 * 用于实现延时消息的延时任务
 */
public class DelayMessageTask extends DelayTask {

    private static final Logger log = LoggerFactory.getLogger(DelayTaskList.class);
    // 该延时消息延时后要投放的topic，因为一开始是先将其投放到延时队列中
    private String topic;
    // 该消息在commitlog中的位置
    private long commitLogOffset;
    private long delayQueueOffset;

    private int queueId;
    // 延时消息对应持久化日志id
    private String logId;
    public DelayMessageTask(BrokerController brokerController) {
        super(brokerController);
    }
    public DelayMessageTask(BrokerController brokerController, String topic, long commitLogOffset,
                            long delayQueueOffset, int queueId, String logId) {
        this(brokerController);
        this.topic = topic;
        this.commitLogOffset = commitLogOffset;
        this.queueId = queueId;
        this.delayQueueOffset = delayQueueOffset;
        this.logId = logId;
    }
    // TODO 该task执行将消息写入retry队列，延时后将其对应消息重新写入commitLog，然后正常提交到consumerQueue，需要记录retry次数
    // TODO 在管理程序中，在执行这个延时任务之前，需要先向wheellog持久化相关信息，便于开机恢复
    @Override
    public void run() {
        // 从commitlog读取对应消息
        StoreInnerMessage message = this.brokerController.getMessageStoreCenter().readSingleMessage(commitLogOffset);

        // 更改消息的主题为指定的topic
        message.setTopic(topic);

        // 设置要投放的消息队列
        message.setMessageQueue(new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), message.getQueueId()));

        // 重新执行消息的投递过程
        PutMessageResponse response = this.brokerController.getMessageStoreCenter().putMessage(message);

        if (response.getResponseType() == StoreResponseType.STORE_OK) {

            // 消息投放成功则向持久化日志写入对应日志的提交标识
            this.brokerController.getMessageStoreCenter().getTimingWheelLog().finishLog(this.logId);
            log.error("{} log has committed", logId);
        }
    }

    @Override
    protected int getTaskType() {
        return DELAY_MESSAGE;
    }
}
