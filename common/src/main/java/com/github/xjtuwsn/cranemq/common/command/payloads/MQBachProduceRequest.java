package com.github.xjtuwsn.cranemq.common.command.payloads;

import lombok.*;
import com.github.xjtuwsn.cranemq.common.entity.Message;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQBachProduceRequest
 * @author:wsn
 * @create:2023/09/27-19:39
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQBachProduceRequest {
    private List<Message> messages;
}
