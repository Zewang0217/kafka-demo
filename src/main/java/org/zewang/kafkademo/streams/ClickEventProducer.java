package org.zewang.kafkademo.streams;


import java.util.Random;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 19:00
 */

@Component
public class ClickEventProducer {

    @Qualifier("optimizedKafkaTemplate")
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final Random random = new Random();

    @Scheduled(fixedRate = 1000) // 每秒生成一个点击事件
    public void sendClickEvent() {
        String userId = "user" + (random.nextInt(5) + 1); // 生成随机用户ID
        String eventType = random.nextBoolean() ? "click" : "view";
        String event = userId + ":" + eventType + ":" + System.currentTimeMillis();

        kafkaTemplate.send(ClickStreamProcessor.CLICK_EVENTS_TOPIC, userId, event);
    }
}
