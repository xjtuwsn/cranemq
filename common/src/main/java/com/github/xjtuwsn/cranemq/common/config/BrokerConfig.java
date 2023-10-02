package com.github.xjtuwsn.cranemq.common.config;

import lombok.*;

/**
 * @project:cranemq
 * @file:BrokerConfig
 * @author:wsn
 * @create:2023/10/02-10:19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class BrokerConfig {
    private String clusterName;
    private String brokerName;

    private int brokerId;

    private FlushDisk flushDisk;
    private int port;

}
