package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

/**
 * @project:cranemq
 * @file:StoreInnerMessage
 * @author:wsn
 * @create:2023/10/03-15:45
 */
public class StoreInnerMessage {

    private String topic;
    private String tag;
    private String id;
    private byte[] body;
    private MessageQueue messageQueue;

    public StoreInnerMessage(Message message, MessageQueue messageQueue, String id) {
        this.messageQueue = messageQueue;
        this.topic = message.getTopic();
        this.tag = message.getTag();
        this.body = message.getBody();
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

    public byte[] getBody() {
        return body;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public String getId() {
        return id;
    }
    public int getQueueId() {
        return messageQueue.getQueueId();
    }
    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}
