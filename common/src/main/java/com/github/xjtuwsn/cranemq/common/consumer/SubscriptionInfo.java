package com.github.xjtuwsn.cranemq.common.consumer;

import lombok.*;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * @project:cranemq
 * @file:SubscriptionInfo
 * @author:wsn
 * @create:2023/10/08-10:39
 */
@Data
@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class SubscriptionInfo implements Serializable {

    private String topic;
    private Set<String> tag;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionInfo that = (SubscriptionInfo) o;
        return Objects.equals(topic, that.topic) && Objects.equals(tag, that.tag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, tag);
    }
}
