package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQLockRespnse
 * @author:wsn
 * @create:2023/10/12-16:25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQLockRespnse implements PayLoad {
    private boolean success;
}
