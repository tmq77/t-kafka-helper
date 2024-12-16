package cn.t.sdk.kafka;

import cn.t.sdk.kafka.config.KafkaProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;

/**
 * @author 陶敏麒
 * @date 2024/10/16 8:56
 */
@Slf4j
public class ProducerContextHolder {

    private final Producer<String, String> producer;

    private String saslUsername;
    private String saslPassword;



    public ProducerContextHolder(String servers) {
        this.producer = new KafkaProducerFactory(servers).buildNewProducer();
    }

    public ProducerContextHolder(String servers, String saslUsername, String saslPassword) {
        this.producer = new KafkaProducerFactory(servers, saslUsername, saslPassword).buildNewProducer();
        this.saslUsername = saslUsername;
        this.saslPassword = saslPassword;
    }

    /**
     * 发送消息
     * @param key 消息的key
     * @param message 消息体
     * @param topic 主题
     */
    public void sendMessage(String key, String message, String topic) {
        this.producer.send(new ProducerRecord<>(topic, key, message), (event, ex) -> {
            if (Objects.nonNull(ex)) {
                log.error("发送消息异常:{}", ex.getMessage());
            }
        });
    }
}
