package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:MQRecordOffsetRequest
 * @author:wsn
 * @create:2023/10/11-15:30
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class MQRecordOffsetRequest implements PayLoad {

    private String group;

    private Map<MessageQueue, Long> offsets;

    public MQRecordOffsetRequest(ConcurrentHashMap<MessageQueue, AtomicLong> origin, String group) {
        this.group = group;
        this.offsets = new HashMap<>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : origin.entrySet()) {
            this.offsets.put(entry.getKey(), entry.getValue().get());
        }
    }
}
