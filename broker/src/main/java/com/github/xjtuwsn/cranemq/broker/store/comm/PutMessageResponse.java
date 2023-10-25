package com.github.xjtuwsn.cranemq.broker.store.comm;

import com.github.xjtuwsn.cranemq.broker.store.MappedFile;
import lombok.*;

/**
 * @project:cranemq
 * @file:PutMessageResponse
 * @author:wsn
 * @create:2023/10/05-16:40
 * 写入信息的响应
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

    private MappedFile mappedFile;

    public PutMessageResponse(StoreResponseType responseType, long offset, int size, MappedFile mappedFile) {
        this.responseType = responseType;
        this.offset = offset;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public PutMessageResponse(StoreResponseType responseType, int queueOffset, MappedFile mappedFile) {
        this.responseType = responseType;
        this.queueOffset = queueOffset;
        this.mappedFile = mappedFile;
    }

    public PutMessageResponse(StoreResponseType responseType) {
        this.responseType = responseType;
    }
}
