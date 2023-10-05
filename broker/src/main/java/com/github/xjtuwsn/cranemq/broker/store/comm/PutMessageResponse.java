package com.github.xjtuwsn.cranemq.broker.store.comm;

import lombok.*;

/**
 * @project:cranemq
 * @file:PutMessageResponse
 * @author:wsn
 * @create:2023/10/05-16:40
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class PutMessageResponse {

    private StoreResponseType responseType;

    private long offset;

    private int size;
    private int queueOffset;

    public PutMessageResponse(StoreResponseType responseType, long offset, int size) {
        this.responseType = responseType;
        this.offset = offset;
        this.size = size;
    }

    public PutMessageResponse(StoreResponseType responseType, int queueOffset) {
        this.responseType = responseType;
        this.queueOffset = queueOffset;
    }

    public PutMessageResponse(StoreResponseType responseType) {
        this.responseType = responseType;
    }
}
