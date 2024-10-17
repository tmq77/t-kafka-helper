package cn.t.sdk.kafka.pool;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 支持多线程traceId的线程池
 * @author 陶敏麒
 * @date 2024/2/1 9:41
 */
@Slf4j
public class SdkThreadPool {

    /**
     * 线程池核心线程数
     */
    private static final int DEFAULT_CORE_SIZE = Runtime.getRuntime().availableProcessors();

    /**
     * 线程池默认最大线程数(IO密集型 2N)
     */
    private static final int DEFAULT_MAX_POOL_SIZE = DEFAULT_CORE_SIZE * 2;

    /**
     * 线程默认空闲存活时间
     */
    private static final long DEFAULT_KEEPALIVE_TIME = 90L;

    /**
     * 线程池工作队列最大上限
     */
    private static final int DEFAULT_MAX_QUEUE_NUM = 2048;

    /**
     * 线程池
     */
    @Getter
    private final ThreadPoolExecutor pool;

    /**
     * 自定义线程工厂
     */
    private final CustomThreadFactory customThreadFactory;

    /**
     * 获取线程工厂创建的线程数
     * @return 线程数
     */
    public long getThreadSize() {
        return this.customThreadFactory.size.get();
    }

    public SdkThreadPool() {
        this.customThreadFactory = new CustomThreadFactory();
        // 参数说明
        // 1、corePoolSize，线程池中的核心线程数
        // 2、maximumPoolSize，线程池中的最大线程数
        // 3、keepAliveTime，空闲时间，当线程池数量超过核心线程数时，多余的空闲线程存活的时间，即：这些线程多久被销毁。
        // 4、unit，空闲时间的单位，可以是毫秒、秒、分钟、小时和天，等等
        // 5、workQueue，等待队列，线程池中的线程数超过核心线程数时，任务将放在等待队列，它是一个BlockingQueue类型的对象
        // 6、threadFactory，线程工厂，我们可以使用它来创建一个线程
        // 7、handler，拒绝策略，当线程池和等待队列都满了之后，需要通过该对象的回调函数进行回调处理
        this.pool = new ThreadPoolExecutor(DEFAULT_CORE_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_KEEPALIVE_TIME,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(DEFAULT_MAX_QUEUE_NUM), this.customThreadFactory,
                new MdcRejectHandler());

        log.info(">>>>>>>>>>当前系统的核心数:{}", DEFAULT_CORE_SIZE);
        log.info(">>>>>>>>>>kafka-helper线程池初始化完成,最大线程数:{}", DEFAULT_MAX_POOL_SIZE);
    }

    /**
     * 执行Runnable任务
     *
     * @param r 线程任务
     */
    public void execute(Runnable r) {
        this.pool.execute(r);
    }

    /**
     * 执行Callable任务
     * @param <T> 类型
     * @param t 线程任务
     * @return Future结果
     */
    public <T> Future<T> submit(Callable<T> t) {
        return this.pool.submit(t);
    }

    /**
     * 关闭线程池
     */
    public void shutdown() {
        this.pool.shutdown();
    }

    /**
     * 立即关闭线程池
     */
    public List<Runnable> shutdownNow() {
        return this.pool.shutdownNow();
    }

    /**
     * 自定义线程工厂
     *
     * @author Administrator
     *
     */
    private static class CustomThreadFactory implements ThreadFactory {

        private static final String NAME_FORMAT = "[-kafka-helper:%s-]";

        /**
         * 线程工厂创建的线程数
         */
        private final AtomicLong size = new AtomicLong(0);

        /**
         * 创建线程,线程池将会从这个方法取得线程
         */
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            // 创建一条线程则计数自增
            t.setName(String.format(NAME_FORMAT, SdkThreadPool.class.getSimpleName() + size.addAndGet(1)));
            return t;
        }
    }

    /**
     * 自定义拒绝策略(当工作队列满的时候执行)
     *
     * @author Administrator
     *
     */
    @Slf4j
    private static class MdcRejectHandler implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                // 在工作队列满的时候进行等待,直到有空余
                log.warn("[kafka-helper线程池]工作队列已满,当前线程阻塞直到有空闲线程");
                executor.getQueue().put(r);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                // 异常则丢弃
                log.error("[kafka-helper线程池]等待加入工作队列时异常", e);
            }
        }
    }
}
