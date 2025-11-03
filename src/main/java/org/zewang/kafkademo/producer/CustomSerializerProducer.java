package org.zewang.kafkademo.producer;


import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.zewang.kafkademo.entity.TestMessage;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/01 19:36
 */

@Slf4j
//@Component
@RequiredArgsConstructor
public class CustomSerializerProducer {

    private final KafkaTemplate<String, TestMessage> customKafkaTemplate;
    private static final String TOPIC = "test-messages";
    private static int messageCounter = 0;

    @Scheduled(fixedDelay = 5000, initialDelay = 15000)
    public void sendTestMessage() {
        messageCounter++;

        try {
            // 创建TestMessage对象
            TestMessage testMessage = new TestMessage();
            testMessage.setMessageId("msg-" + System.currentTimeMillis() + "-" +
                UUID.randomUUID().toString().substring(0, 8));
            testMessage.setContent("测试消息 #" + messageCounter + " 内容: " +
                UUID.randomUUID().toString());
            testMessage.setSender("sender-" + ThreadLocalRandom.current().nextInt(1, 5));
            testMessage.setReceivedTime(LocalDateTime.now());
            testMessage.setStatus("PENDING");
            testMessage.setProcessCount(0);

            // 发送对象
            customKafkaTemplate.send(TOPIC, "key-" + messageCounter, testMessage)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("成功发送TestMessage到主题 '{}': messageId={}",
                            TOPIC, testMessage.getMessageId());
                    } else {
                        log.error("发送TestMessage失败：messageId={}, error={}",
                            testMessage.getMessageId(), ex.getMessage());
                    }
                });
        } catch (Exception e) {
            log.error("构造或发送消息时发送错误：error={}", e.getMessage(), e);
        }
    }
}
