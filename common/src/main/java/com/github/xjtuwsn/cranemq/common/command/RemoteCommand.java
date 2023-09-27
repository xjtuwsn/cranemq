package com.github.xjtuwsn.cranemq.common.command;

import lombok.*;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:RemoteCommand
 * @author:wsn
 * @create:2023/09/27-10:18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RemoteCommand implements Serializable {
    private static final long serialVersionUID = 23L;
    private Header header;
    private PayLoad payLoad;
}
