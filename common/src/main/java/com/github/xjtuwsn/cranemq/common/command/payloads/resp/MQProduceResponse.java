package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import lombok.*;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:MQProduceResponse
 * @author:wsn
 * @create:2023/10/02-16:17
 */
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQProduceResponse implements Serializable, PayLoad {

    private String message;
}
