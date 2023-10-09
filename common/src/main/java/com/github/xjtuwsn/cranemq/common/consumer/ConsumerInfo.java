package com.github.xjtuwsn.cranemq.common.consumer;

import lombok.*;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @project:cranemq
 * @file:ConsumerInfo
 * @author:wsn
 * @create:2023/10/08-10:41
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class ConsumerInfo implements Serializable {

    private String consumerGroup;
    private MessageModel messageModel;
    private StartConsume startConsume;
    private Set<SubscriptionInfo> subscriptionInfos;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerInfo that = (ConsumerInfo) o;
        return Objects.equals(consumerGroup, that.consumerGroup) && messageModel == that.messageModel
                && startConsume == that.startConsume && Objects.equals(subscriptionInfos, that.subscriptionInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroup, messageModel, startConsume, subscriptionInfos);
    }
}
