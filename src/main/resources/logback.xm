<configuration scan="false">

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <!-- 格式化输出： %d表示日期， %thread表示线程名， %-5level: 级别从左显示5个字符宽度 %msg:日志消息, %n是换行符 -->
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS}[%thread] %-5level %logger{50} - %msg%n</pattern>
        </encoder>
    </appender>

<!--<logger name="org.apache.kafka" level="INFO" />-->

</configuration>