package cn.t.sdk.kafka.data;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.header.Headers;

/**
 * 消息
 * @author 陶敏麒
 * @date 2024/10/15 17:27
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MessageDto {
    // 消息id
    private String messageId;
    // 消息
    private String message;
    private String topic;
    private long offset;
    private Headers headers;
}
