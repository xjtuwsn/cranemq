package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;

/**
 * @project:cranemq
 * @file:PullRequest
 * @author:wsn
 * @create:2023/10/08-17:10
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class PullRequest {

    private String groupName;
    private MessageQueue messageQueue;
    private BrokerQueueSnapShot snapShot;

    private long offset;

    @Override
    public String toString() {
        return "PullRequest{" +
                "groupName='" + groupName + '\'' +
                ", messageQueue=" + messageQueue +
                ", offset=" + offset +
                '}';
    }
}
