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
    private byte[] body;
}
