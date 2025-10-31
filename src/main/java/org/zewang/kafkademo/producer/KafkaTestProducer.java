package org.zewang.kafkademo.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;

import java.util.concurrent.TimeUnit;

//@Component
public class KafkaTestProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestProducer.class);
    private static final String TOPIC = "web-logs"; // 必须与 Consumer 监听的主题一致

    private final KafkaTemplate<String, String> kafkaTemplate;

    // 假设的常见访问日志格式模板，与 KafkaLogConsumer 中的正则表达式匹配
    // 格式: IP - - [dd/MMM/yyyy:HH:mm:ss +0000] "METHOD PATH HTTP/1.1" STATUS_CODE RESPONSE_TIME_MS "USER_AGENT"
    private static final String[] MOCK_LOGS = {
        "10.0.0.1 - - [30/Oct/2025:10:30:05 +0800] \"GET /api/users/123 HTTP/1.1\" 200 15ms \"Mozilla/5.0 (Windows NT 10.0)\"",
        "192.168.1.5 - - [30/Oct/2025:10:30:10 +0800] \"POST /login HTTP/1.1\" 401 250ms \"Curl/7.64.1\"",
        "203.0.113.8 - - [30/Oct/2025:10:30:15 +0800] \"GET /images/logo.png HTTP/1.1\" 200 5ms \"Mobile Safari\"",
        "10.0.0.1 - - [30/Oct/2025:10:30:20 +0800] \"DELETE /admin/temp HTTP/1.1\" 404 120ms \"NodeJS Agent\"",
        // 一个格式不匹配的错误消息，用于测试 Consumer 的异常处理
        "This is an unparsable message for error testing.",
    };

    public KafkaTestProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Spring Boot 应用启动后自动运行
    @PostConstruct
    public void sendMockMessages() {
        log.info("--- Starting mock log message production to topic: {} ---", TOPIC);

        // 异步发送消息
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(5); // 等待 Consumer 启动
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (String logMessage : MOCK_LOGS) {
                kafkaTemplate.send(TOPIC, logMessage)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send message: {}", logMessage, ex);
                        } else {
                            log.debug("Sent mock log: {}", logMessage);
                        }
                    });
            }
            log.info("--- Finished sending mock messages. Check MySQL and Consumer logs. ---");
        }).start();
    }
}