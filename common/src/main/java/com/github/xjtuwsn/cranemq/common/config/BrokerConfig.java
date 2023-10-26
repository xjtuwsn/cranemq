package com.github.xjtuwsn.cranemq.common.config;

import com.github.xjtuwsn.cranemq.common.exception.CraneBrokerException;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import lombok.*;

import java.util.HashMap;
import java.util.Map;

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

    private String registry;

    private long keepAliveTime = 1000 * 120;

    private long longPollingTime = 1000 * 15;

    private RegistryType registryType = RegistryType.DEFAULT;

    private int maxRetryTime = 15;

    // private String retryLevel = "5,10,15,20,25,30,60,120,240,480,960,1200,3000,6000,12000";
    private String retryLevel = "5,5,5,5,5,5,5,5,5,5,5,5,5,5,5";
    private Map<Integer, Long> retryMap = new HashMap<>();

    public void initRetry() {
        if (retryMap == null || "".equals(retryLevel)) {
            throw new CraneBrokerException("Retry str error");
        }
        String[] levels = retryLevel.split(",");
        if (maxRetryTime < 0 || maxRetryTime != levels.length) {
            throw new CraneBrokerException("Retry time error");
        }
        for (int i = 0; i < levels.length; i++) {
            retryMap.put(i + 1, Long.valueOf(levels[i]));
        }
    }

    public long delayTime(int retry) {
        return Math.max(retryMap.get(retry) - 1, 0);
    }

}
