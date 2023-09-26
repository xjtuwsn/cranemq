package xjtu.wsn.cranemq.common.request;

import lombok.*;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:BaseRequest
 * @author:wsn
 * @create:2023/09/26-20:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class BaseRequest implements Serializable {
    private static final long serialVersionUID = 23L;
    private RequestType dataType;
    private PayLoad data;
    private String requestID;


}
