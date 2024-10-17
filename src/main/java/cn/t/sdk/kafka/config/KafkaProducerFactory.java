package cn.t.sdk.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author 陶敏麒
 * @date 2024/10/16 8:47
 */
public class KafkaProducerFactory {

    // kafka服务器地址,逗号分隔
    private final String servers;

    public KafkaProducerFactory(String servers) {
        this.servers = servers;
    }

    /**
     * 初始化kafka配置 - 固定的一些配置
     */
    private Properties fixedProperties() {
        // 参考 https://kafka.apache.org/documentation/#consumerconfigs
        final Properties props = new Properties();
        // 服务器地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.servers);
        // 序列化配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        // ack模式
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    public Producer<String, String> buildNewProducer() {
        final Producer<String, String> producer = new KafkaProducer<>(this.fixedProperties());
        return producer;
    }
}
