package com.github.xjtuwsn.cranemq.common.entity;

import lombok.*;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:Message
 * @author:wsn
 * @create:2023/09/27-16:14
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Message implements Serializable {
    private static final long serialVersionUID = 23L;
    private String topic;
    private String tag;
    private byte[] body;
    public Message(String topic, byte[] body) {
        this(topic, "", body);
    }
    public Message(Message other) {
        this.topic = other.getTopic();
        this.tag = other.getTag();
        this.body = other.getBody();
    }
}
