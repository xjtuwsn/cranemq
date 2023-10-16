package com.github.xjtuwsn.cranemq.common.route;

import lombok.*;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:QueueData
 * @author:wsn
 * @create:2023/09/28-20:09
 */
@ToString
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Data
public class QueueData implements Serializable {
    private String broker;
    private int readQueueNums;
    private int writeQueueNums;

    public QueueData(String broker) {
        this.broker = broker;
    }

    public QueueData(int number) {
        this.readQueueNums = number;
        this.writeQueueNums = number;
    }
}
