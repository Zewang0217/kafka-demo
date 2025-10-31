package org.zewang.kafkademo.producer;


import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 21:09
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class TestProducer {

    @Qualifier("kafkaTemplateAcks1") // 指定自动装配类型
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "test-topic";

    // 定时发送测试消息
    @Scheduled(fixedDelay = 5000, initialDelay = 10000) // 每5秒发送一次，初始延迟10秒
    public void sendTestMessage() {
        // 构造测试消息
        String key = "key-" + ThreadLocalRandom.current().nextInt(1, 10);
        String message = "测试消息，ID为：" + UUID.randomUUID().toString();

        kafkaTemplate.send(TOPIC, key, message)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("成功发送消息 '{}' 到主题 '{}'，key为 '{}'", message, TOPIC, key);
                } else {
                    log.error("发送消息失败：{}", ex.getMessage());
                }
            });
    }
}
