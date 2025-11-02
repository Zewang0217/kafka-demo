// TestMessageProducer.java
package org.zewang.kafkademo.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 测试消息生产者，定期向Kafka发送测试消息
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/1 10:10
 */
@Slf4j
//@Component
public class TestMessageProducer {

//    @Qualifier("kafkaTemplateAcks1") // 指定自动装配类型
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "test-messages";
    private static int messageCounter = 0;

    public TestMessageProducer(@Qualifier("kafkaTemplateAcks1") KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 定时发送测试消息
     * 每3秒发送一次，初始延迟5秒
     */
    @Scheduled(fixedDelay = 3000, initialDelay = 5000)
    public void sendTestMessage() {
        messageCounter++;

        try {
            // 构造测试消息内容
            String key = "sender-" + ThreadLocalRandom.current().nextInt(1, 5);
            String messageId = "msg-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8);
            String content = "测试消息 #" + messageCounter + " 内容: " + UUID.randomUUID().toString();

            // 构造JSON格式的消息
            String message = String.format(
                "{\"messageId\":\"%s\",\"content\":\"%s\",\"sender\":\"%s\"}",
                messageId, content, key
            );

            kafkaTemplate.send(TOPIC, key, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("成功发送测试消息到主题 '{}': messageId={}, key={}", TOPIC, messageId, key);
                    } else {
                        log.error("发送测试消息失败: messageId={}, error={}", messageId, ex.getMessage());
                    }
                });
        } catch (Exception e) {
            log.error("构造或发送消息时发生错误: error={}", e.getMessage(), e);
        }
    }
}
