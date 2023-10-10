package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import lombok.*;

import java.util.Set;

/**
 * @project:cranemq
 * @file:MQReblanceQueryRequest
 * @author:wsn
 * @create:2023/10/10-16:15
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MQReblanceQueryRequest implements PayLoad {
    
    private String clientId;
    
    private String group;
    
    private Set<String> topics;
}
