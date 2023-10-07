package com.github.xjtuwsn.cranemq.common.remote;

import lombok.*;

/**
 * @project:cranemq
 * @file:RemoteAddress
 * @author:wsn
 * @create:2023/09/26-20:39
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RemoteAddress {
    private String address;

    private int port;

}
