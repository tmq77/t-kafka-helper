package cn.t.sdk.kafka;

import cn.t.sdk.kafka.config.KafkaDynamicTopicConsumerFactory;
import cn.t.sdk.kafka.data.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 消费者持有者
 * @author 陶敏麒
 * @date 2024/10/15 15:05
 */
@Slf4j
public class ConsumerContextHolder {

    // 持有一个消费者
    private final Consumer<String, String> consumer;
    // 持有一个消费者线程
    private Thread cosumerThread;
    // 自定义线程
    private final Supplier<Thread> consumerThreadSupplier = () -> new Thread(this::executeThread);
    // 业务方法
    private final java.util.function.Consumer<List<MessageDto>> bizConsumer;

    private Set<String> topics;

    private final ReentrantLock lock  = new ReentrantLock();

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
     * 创建消费者并订阅主题 - 不允许并发
     * @param topics 主题列表
     */
    public synchronized void subscribe(Set<String> topics) {
        log.info(">>>>准备开始订阅主题,当前线程:{}", Thread.currentThread().getName());
        if (Objects.isNull(topics) || topics.isEmpty()) {
            log.warn("消费主题为空,不进行消费");
            return;
        }
        if (Objects.nonNull(this.cosumerThread)
                && (Thread.State.NEW.equals(this.cosumerThread.getState())
                || Thread.State.RUNNABLE.equals(this.cosumerThread.getState()))) {
            log.warn(">>>>并发!!已在创建线程中!!");
            return;
        }
        this.topics = topics;
        // 获取一个新线程
        this.cosumerThread = this.consumerThreadSupplier.get();
        this.consumer.subscribe(this.topics);
        // 开启消费线程开始消费
        this.cosumerThread.start();
        log.info(">>>>开始准备消费者...本次订阅的主题为:{},当前线程:{}",topics, Thread.currentThread().getName());
    }

    /**
     * 取消订阅
     */
    public synchronized void unsubscribe() {
        if (Objects.isNull(this.cosumerThread)) {
            // 没启动的/打断的则直接认为成功
            return;
        }

        // 终止了
        if (!Thread.State.TERMINATED.equals(this.cosumerThread.getState())) {
            this.cosumerThread.interrupt();
            // 打断线程
            log.warn(">>>>取消订阅,消费者即将终止");
        } else {
            log.warn(">>>>消费者已终止,忽略");
        }
        // 这里立即设置为false,执行完后线程会立即结束
        // 取消订阅 - 消费者线程不安全,需要在同一线程中取消
        // this.consumer.unsubscribe();
        // 在这里延时一下,避免线程未完全停止就创建新的线程导致异常
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 执行线程消费 - 保证整个消费者在一个线程里面
     */
    private void executeThread() {

        try {
            if (Objects.isNull(this.topics) || this.topics.isEmpty()) {
                log.warn("消费主题为空,不进行消费");
                Thread.currentThread().interrupt();
                return;
            }

            log.info(">>>>开始执行消费者消费线程,当前线程：{}", Thread.currentThread().getName());
            // 不使用sleep之类的方法时, InterruptedException异常被捕获时不会复位,
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
            // 操作consumer需要在同一个线程中 - 取消订阅的时候可能也在poll，所以要保证串行
            this.consumer.unsubscribe();
            // 外部无论打断做少次,running只会在这里重置
            log.warn(">>>>消费者已停止消费,当前线程:{}", Thread.currentThread().getName());
        } finally {
            // 操作consumer需要在同一个线程中 - 取消订阅的时候可能也在poll，所以要保证串行
            this.consumer.unsubscribe();
            log.warn(">>>>消费者已停止监听主题,当前线程:{}", Thread.currentThread().getName());
        }
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
