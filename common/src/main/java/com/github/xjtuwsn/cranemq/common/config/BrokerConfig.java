package com.github.xjtuwsn.cranemq.common.config;

import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
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

    private int port;

    private String registrys;

    private long keepAliveTime = 1000 * 120;

    private long longPollingTime = 1000 * 15;

    private RegistryType registryType = RegistryType.DEFAULT;

}
