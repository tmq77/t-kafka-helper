package cn.t.sdk.kafka;

import cn.t.sdk.kafka.config.KafkaDynamicTopicConsumerFactory;
import cn.t.sdk.kafka.data.MessageDto;
import cn.t.sdk.kafka.pool.SdkThreadPool;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 消费者持有者
 * @author 陶敏麒
 * @date 2024/10/15 15:05
 */
@Slf4j
public class ConsumerContextHolder {

    // 客户端是否正在运行
    private boolean running;
    // 持有一个消费者
    private final Consumer<String, String> consumer;
    // 持有一个消费者线程
    private Thread cosumerThread;
    // 自定义线程
    private final Supplier<Thread> consumerThreadSupplier = () -> new Thread(this::executeThread);
    // 业务方法
    private final java.util.function.Consumer<List<MessageDto>> bizConsumer;

    private final SdkThreadPool sdkThreadPool = new SdkThreadPool();

    /**
     * 创建一个单一kafka集群的消费者持有者 - 一个holder就一个消费者
     * @param servers kafka集群地址,逗号分隔
     * @param groupId 消费者组id
     */
    public ConsumerContextHolder(String servers, String groupId, java.util.function.Consumer<List<MessageDto>> bizConsumer) {
        this.consumer = new KafkaDynamicTopicConsumerFactory(servers, groupId).buildNewConsumer();
        this.bizConsumer = bizConsumer;
    }

    /**
     * 创建一个单一kafka集群的消费者持有者 - 一个holder就一个消费者
     * @param servers kafka集群地址,逗号分隔
     * @param groupId 消费者组id
     */
    public ConsumerContextHolder(String servers, String groupId, String username, String password,java.util.function.Consumer<List<MessageDto>> bizConsumer) {
        this.consumer = new KafkaDynamicTopicConsumerFactory(servers, groupId, username, password).buildNewConsumer();
        this.bizConsumer = bizConsumer;
    }

    /**
     * 创建消费者并订阅主题
     * @param topics 主题列表
     */
    public synchronized void subscribe(Set<String> topics) {
        if (Objects.isNull(topics) || topics.isEmpty()) {
            log.warn("消费主题为空,不进行消费");
            throw new RuntimeException("消费主题不能为空");
        }

        // 校验是否已经在跑了
        if (this.running) {
            log.warn("当前消费者正在消费,需要先停止消费...");
            throw new RuntimeException("当前消费者正在消费,需要先停止消费...");
        }

        this.consumer.subscribe(topics);
        // 获取一个新线程
        this.cosumerThread = this.consumerThreadSupplier.get();
        // 开启消费线程开始消费
        this.cosumerThread.start();
        log.info(">>>>开始准备消费者...");
    }

    /**
     * 取消订阅
     */
    public synchronized boolean unsubscribe() {
        if (!this.running) {
            // 没启动的则直接认为成功
            return true;
        }
        // 打断线程
        log.warn(">>>>取消订阅,消费者即将终止");
        this.cosumerThread.interrupt();
        // 取消订阅 - 消费者线程不安全,需要在同一线程中取消
        // this.consumer.unsubscribe();
        this.running = false;
        // 在这里延时一下,避免线程未完全停止就创建新的线程导致异常
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return true;
    }

    /**
     * 执行线程消费
     */
    private synchronized void executeThread() {

        if (this.running) {
            log.info(">>>>消费者线程已在执行...");
            return;
        }

        log.info(">>>>开始执行消费者消费线程");
        // 循环消费
        this.running = true;
        // 不使用使用sleep之类的方法时, InterruptedException异常被捕获时不会复位,
        while (!Thread.currentThread().isInterrupted()) {
            // 减少CPU占用
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(1500));

                if (records.isEmpty()) {
                    continue;
                }

                List<MessageDto> messageList = new ArrayList<>();
                for (ConsumerRecord<String, String> msgRecord : records) {
                    messageList.add(new MessageDto(msgRecord.key(), msgRecord.value(), msgRecord.topic(), msgRecord.offset(), msgRecord.headers()));
                }
                try {
                    // 批量消费
                    this.bizConsumer.accept(messageList);
                } catch (Exception e) {
                    log.error("消费消息处理异常", e);
                }

                if (!records.isEmpty()) {
                    // 异步提交
                    this.consumer.commitAsync();
                }

            } catch (InterruptedException e) {
                // 重置线程中断状态
                Thread.currentThread().interrupt();
            }
        }
        // 操作consumer需要在同一个线程中
        this.consumer.unsubscribe();
        log.warn(">>>>消费者已停止消费");
    }


    public static void main(String[] args) {
        ProducerContextHolder producer = new ProducerContextHolder("10.10.44.176:9094", "root", "root");
        producer.sendMessage("aaa", "hahaha11a", "test");
        ConsumerContextHolder consumerContextHolder = new ConsumerContextHolder("10.10.44.176:9094", "test-ipaas1","root", "root", msg -> {
            System.out.println(msg);
        });
        consumerContextHolder.subscribe(Set.of("test"));
        consumerContextHolder.unsubscribe();
    }

}
