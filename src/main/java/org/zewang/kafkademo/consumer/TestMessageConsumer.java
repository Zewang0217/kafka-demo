// TestMessageConsumer.java
package org.zewang.kafkademo.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.zewang.kafkademo.entity.TestMessage;
import org.zewang.kafkademo.service.RedisDedupService;
import org.zewang.kafkademo.service.TestMessageService;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 测试消息消费者，演示手动提交offset和去重处理
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/1 10:20
 */
@Slf4j
//@Component
@RequiredArgsConstructor
public class TestMessageConsumer {

    private final TestMessageService testMessageService;
    private final RedisDedupService redisDedupService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicInteger messageCounter = new AtomicInteger(0);

    @PostConstruct
    public void init() {
        log.info("TestMessageConsumer 初始化完成");
    }

    /**
     * 监听test-messages主题的消息
     * @param record Kafka消息记录
     * @param acknowledgment 手动确认对象
     */
//    @KafkaListener(topics = "test-messages", groupId = "test-message-group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String key = record.key();
        String value = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.info("接收到测试消息: key={}, partition={}, offset={}, value={}", key, partition, offset, value);

        try {
            // 解析消息内容
            TestMessageDto messageDto = objectMapper.readValue(value, TestMessageDto.class);
            log.info("成功解析消息: messageId={}", messageDto.getMessageId());

            // 使用Redis检查消息是否已处理（去重）
            if (redisDedupService.isMessageProcessed(messageDto.getMessageId())) {
                log.warn("检测到重复消息，已跳过处理: messageId={}", messageDto.getMessageId());
                acknowledgment.acknowledge(); // 提交offset
                return;
            }

            // 模拟业务处理
            processMessage(messageDto);

            // 标记消息已处理到Redis
            redisDedupService.markMessageAsProcessed(messageDto.getMessageId());

            // 手动提交offset
            acknowledgment.acknowledge();
            log.info("消息处理完成并提交offset: messageId={}, offset={}", messageDto.getMessageId(), offset);

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("JSON解析失败，跳过此消息: value={}, error={}", value, e.getMessage());
            // 对于无法解析的消息，记录错误并提交offset，避免无限重试
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("处理消息时发生错误: key={}, value={}, error={}", key, value, e.getMessage(), e);
            // 重新抛出异常以便重试机制处理
            throw new RuntimeException("消息处理失败: " + e.getMessage(), e);
        }
    }


    /**
     * 处理消息业务逻辑
     * @param messageDto 消息数据传输对象
     * @throws Exception 处理异常
     */
    private void processMessage(TestMessageDto messageDto) throws Exception {
        log.debug("开始处理消息: messageId={}", messageDto.getMessageId());

        int count = messageCounter.incrementAndGet();
        log.debug("当前消息计数: {}", count);

        // 模拟业务处理失败 - 每5条消息中有1条会失败
        if (count % 5 == 0) {
            log.warn("模拟业务处理失败: messageId={}", messageDto.getMessageId());
            throw new RuntimeException("模拟业务处理失败: messageId=" + messageDto.getMessageId());
        }

        // 保存消息到数据库
        TestMessage testMessage = new TestMessage();
        testMessage.setMessageId(messageDto.getMessageId());
        testMessage.setContent(messageDto.getContent());
        testMessage.setSender(messageDto.getSender());
        testMessage.setReceivedTime(LocalDateTime.now());
        testMessage.setStatus("PROCESSED");
        testMessage.setProcessCount(1);

        log.debug("准备保存消息到数据库: messageId={}", messageDto.getMessageId());
        testMessageService.saveMessage(testMessage);
        log.debug("消息保存完成: messageId={}", messageDto.getMessageId());

        // 模拟处理耗时
        Thread.sleep(100);
        log.info("消息处理成功: messageId={}", messageDto.getMessageId());
    }


    /**
     * 消息数据传输对象
     */
    @Data
    private static class TestMessageDto {
        // Getters and Setters
        private String messageId;
        private String content;
        private String sender;
    }
}
