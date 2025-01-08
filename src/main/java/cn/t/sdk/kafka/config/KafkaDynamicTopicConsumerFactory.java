package cn.t.sdk.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Objects;
import java.util.Properties;


/**
 * kafka的动态主题消费者工厂
 * @author 陶敏麒
 * @date 2024/10/15 14:40
 */
public class KafkaDynamicTopicConsumerFactory {

    // 消费者组id
    private final String groupId;
    // kafka服务器地址,逗号分隔
    private final String servers;

    private String saslUsername;
    private String saslPassword;

    private boolean latest;

    /**
     * 指定服务地址和消费者组
     * @param servers 服务地址 localhost:9092  多个则逗号分隔
     * @param groupId 消费者组id
     */
    public KafkaDynamicTopicConsumerFactory(String servers, String groupId) {
        this.servers = servers;
        this.groupId = groupId;
    }

    /**
     * 指定服务地址和消费者组
     * @param servers 服务地址 localhost:9092  多个则逗号分隔
     * @param groupId 消费者组id
     */
    public KafkaDynamicTopicConsumerFactory(String servers, String groupId, String saslUsername, String saslPassword) {
        this.servers = servers;
        this.groupId = groupId;
        this.saslUsername = saslUsername;
        this.saslPassword = saslPassword;
    }

    /**
     * 初始化kafka配置 - 固定的一些配置
     */
    private Properties fixedProperties(boolean latest) {
        // 参考 https://kafka.apache.org/documentation/#consumerconfigs
        final Properties props = new Properties();
        // 服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.servers);
        // 序列化配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        // 最大拉取数量 - 5M  - 默认是50M
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 5242880);
        // 最大拉去的数据条数 - 默认500
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 消费者id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        // earliest 从最开始的位置开始消费
        // latest 从最新的位置开始消费
        if (latest) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase());
        } else {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        }

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

    /**
     * 构建一个kafka消费者<br/>
     * 默认从最新的offset进行消费,默认使用String进行(反)序列化<br/>
     * 默认1秒拉取一次<br/>
     * @return 新的kafka消费者
     */
    public Consumer<String, String> buildNewConsumer(boolean latest) {
        Properties fixedProps = this.fixedProperties(latest);
        // 创建消费者
        return new KafkaConsumer<>(fixedProps);
    }
}
