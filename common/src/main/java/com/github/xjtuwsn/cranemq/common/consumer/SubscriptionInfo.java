package com.github.xjtuwsn.cranemq.common.entity;

import lombok.*;

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
public class SubscriptionInfo {

    private String topic;
    private Set<String> tag;
}
