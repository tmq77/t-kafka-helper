package cn.t.sdk.kafka.data;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

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

}
