package cn.t.sdk.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

/**
 * @author 陶敏麒
 * @date 2024/10/16 8:47
 */
public class KafkaProducerFactory {

    // kafka服务器地址,逗号分隔
    private final String servers;

    private String saslUsername;
    private String saslPassword;

    public KafkaProducerFactory(String servers) {
        this.servers = servers;
    }

    public KafkaProducerFactory(String servers, String saslUsername, String saslPassword) {
        this.servers = servers;
        this.saslUsername = saslUsername;
        this.saslPassword = saslPassword;
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

        if (Objects.nonNull(this.saslUsername) && !this.saslUsername.isBlank() && Objects.nonNull(this.saslPassword) && !this.saslPassword.isBlank()) {
            // 配置 SASL/PLAIN 认证
            // props.put("security.protocol", "SASL_SSL"); // 使用SASL_SSL需要指定证书
            props.put("security.protocol", "SASL_PLAINTEXT"); // 或者 SASL_SSL 根据你的 Kafka 配置

            // 下面的配置使用SASL_SSL需要使用
            // 关闭https校验,默认会打开
            // props.put("ssl.endpoint.identification.algorithm", "");
            // 配置一个认证主题名称,sasl一般使用kafka即可，有自定义认证的可以配置对应域名
            // props.put("sasl.kerberos.service.name", "kafka");
            // props.put("ssl.truststore.location", "E:/kafka.truststore.jks");
            // props.put("ssl.truststore.password", "certificatePassword123");

            // 下面的配置 两个协议通用
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + this.saslUsername + "\" password=\"" + this.saslPassword + "\";");
        }

        return props;
    }

    public Producer<String, String> buildNewProducer() {
        final Producer<String, String> producer = new KafkaProducer<>(this.fixedProperties());
        return producer;
    }
}
