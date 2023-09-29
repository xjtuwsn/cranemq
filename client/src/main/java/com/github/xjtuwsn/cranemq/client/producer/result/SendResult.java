package com.github.xjtuwsn.cranemq.client.producer.result;

import lombok.*;

/**
 * @project:cranemq
 * @file:SendResult
 * @author:wsn
 * @create:2023/09/27-19:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SendResult {
    private SendResultType resultType;
    private String correlationID;

}
