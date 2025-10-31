package org.zewang.kafkademo.consumer;


import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.zewang.kafkademo.service.RedisDedupService;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 21:14
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class ManualOffsetConsumer {

    private final AtomicInteger messageCounter = new AtomicInteger(0);
    private final RedisDedupService redisDedupService;

//    @KafkaListener(topics = "test-topic", groupId = "manual-offset-group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String key = record.key();
        String value = record.value();
        int partition = record.partition();
        long offset = record.offset();

        String messageId = generateMessageId(key, value, partition, offset);

        log.info("成功收到消息：key={}, value={}, partition={}, offset={}", key, value, partition, offset);

        try {
            // 模拟业务处理
            processSingleMessage(value);

            // 标记消息已处理
            redisDedupService.markMessageAsProcessed(messageId);

            // 手动提交 offset
            acknowledgment.acknowledge();
            log.info("消息处理完成；并成功提交 offset：{}", offset);
        } catch (Exception e) {
            log.error("消息处理失败：{}", e.getMessage());
            // 不提交 offset， 让消息可以被重新消费
            // 注意：实际应用中，此处应该有重试机制和死信队列处理
            System.err.println("模拟业务处理出现错误，未提交 offset：" + offset);
        }
    }

    private void processSingleMessage(String message) throws Exception {
        int count = messageCounter.incrementAndGet();

        // 模拟业务失败 - 每五条消息失败一条
        if (count % 5 == 0) {
            throw new RuntimeException("模拟业务处理失败");
        }

        // 模拟业务耗时
        Thread.sleep(100);
        log.info("消息处理成功");
    }

    // 生成消息唯一标识
    private String generateMessageId(String key, String value, int partition, long offset) {
        return key + "_" + partition + "_" + offset;
    }
}
