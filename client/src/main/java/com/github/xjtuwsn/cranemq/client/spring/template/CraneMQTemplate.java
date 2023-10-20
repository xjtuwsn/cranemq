package com.github.xjtuwsn.cranemq.client.spring.template;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.serialize.Serializer;
import com.github.xjtuwsn.cranemq.common.remote.serialize.impl.Hessian1Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:CraneTemplate
 * @author:wsn
 * @create:2023/10/14-15:03
 */
public class CraneMQTemplate {

    private static final Logger log = LoggerFactory.getLogger(CraneMQTemplate.class);

    private Serializer serializer;

    private DefaultMQProducer defaultMQProducer;

    public CraneMQTemplate(DefaultMQProducer defaultMQProducer, Serializer serializer) {
        this.defaultMQProducer = defaultMQProducer;
        this.serializer = serializer;
    }

    public<T> SendResult send(String topic, String tag, T... data) {
        check(topic, tag, data);
        if (data.length == 1) {
            byte[] body = getByteData(data[0]);
            Message message = new Message(topic, tag, body);
            return defaultMQProducer.send(message);
        } else {
            List<Message> messages = new ArrayList<>();
            for (Object e : data) {
                byte[] body = getByteData(data);
                Message message = new Message(topic, tag, body);
                messages.add(message);
            }
            return defaultMQProducer.send(messages);
        }
    }
    public<T> void send(String topic, String tag, SendCallback callback, T... data) {
        check(topic, tag, data);
        if (data.length == 1) {
            byte[] body = getByteData(data[0]);
            Message message = new Message(topic, tag, body);
            defaultMQProducer.send(message, callback);
        } else {
            List<Message> messages = new ArrayList<>();
            for (Object e : data) {
                byte[] body = getByteData(data);
                Message message = new Message(topic, tag, body);
                messages.add(message);
            }
            defaultMQProducer.send(messages, callback);
        }
    }

    public<T> void sendOneWay(String topic, String tag, T... data) {
        check(topic, tag, data);
        if (data.length == 1) {
            byte[] body = getByteData(data[0]);
            Message message = new Message(topic, tag, body);
            defaultMQProducer.send(message, true);
        } else {
            List<Message> messages = new ArrayList<>();
            for (Object e : data) {
                byte[] body = getByteData(data);
                Message message = new Message(topic, tag, body);
                messages.add(message);
            }
            defaultMQProducer.send(messages, true);
        }
    }

    public<T> SendResult send(String topic, String tag, T data, MQSelector selector, Object arg) {
        check(topic, tag, data);
        if (selector == null || arg == null) {
            throw new CraneClientException("Selector or arg can not be null");
        }
        byte[] body = getByteData(data);
        Message message = new Message(topic, tag, body);
        return defaultMQProducer.send(message, selector, arg);


    }
    public<T> SendResult send(String topic, String tag, T data, long delay, TimeUnit unit) {
        check(topic, tag, data);
        byte[] body = getByteData(data);
        Message message = new Message(topic, tag, body);
        return defaultMQProducer.send(message, delay, unit);
    }

    private<T> byte[] getByteData(T data) {
        try {
            return serializer.serialize(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private<T> void check(String topic, String tag, T... datas) {
        if (datas == null || StrUtil.isEmpty(topic) || tag == null) {
            throw new CraneClientException("Send data can not be null");
        }
        if (topic.length() > 255 || tag.length() > 255) {
            throw new CraneClientException("Topic or tag is too long");
        }
        for (T data : datas) {
            if (!Serializable.class.isAssignableFrom(data.getClass())) {
                throw new CraneClientException("Send data must be Serializable");
            }
        }
    }
}
