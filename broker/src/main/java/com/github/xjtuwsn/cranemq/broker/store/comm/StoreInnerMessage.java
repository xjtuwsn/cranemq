package com.github.xjtuwsn.cranemq.broker.store.comm;

import cn.hutool.crypto.digest.otp.TOTP;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

/**
 * @project:cranemq
 * @file:StoreInnerMessage
 * @author:wsn
 * @create:2023/10/03-15:45
 * 内部进行存储的消息，进行了相应的包装
 */
public class StoreInnerMessage {

    private String topic;
    private String tag;
    private String id;
    private byte[] body;
    private MessageQueue messageQueue;
    private int queueId;

    private long delay;

    private int retry;
    public StoreInnerMessage(Message message, String id, long delay) {
        this.topic = message.getTopic();
        this.tag = message.getTag();
        this.body = message.getBody();
        this.id = id;
        this.delay = delay;
    }

    public StoreInnerMessage(Message message, MessageQueue messageQueue, String id, long delay) {
        this.messageQueue = messageQueue;
        this.topic = message.getTopic();
        this.tag = message.getTag();
        this.body = message.getBody();
        this.id = id;
        this.delay = delay;
    }

    public StoreInnerMessage(String topic, String tag, String id, byte[] body, int retry, int queueId) {
        this.topic = topic;
        this.tag = tag;
        this.id = id;
        this.body = body;
        this.retry = retry;
        this.queueId = queueId;
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
        return queueId;
    }
    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getDelay() {
        return delay;
    }

    public int getRetry() {
        return retry;
    }
    public Message getMessage() {
        return new Message(topic, tag, body);
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    @Override
    public String toString() {
        return "StoreInnerMessage{" +
                "topic='" + topic + '\'' +
                ", tag='" + tag + '\'' +
                ", id='" + id + '\'' +
                ", queueId=" + queueId +
                ", delay=" + delay +
                ", retry=" + retry +
                '}';
    }
}
