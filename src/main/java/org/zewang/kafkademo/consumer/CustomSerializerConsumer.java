package org.zewang.kafkademo.consumer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.zewang.kafkademo.entity.TestMessage;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 自定义序列化器的消费者
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/01 20:50
 */

@Slf4j
//@Component
@RequiredArgsConstructor
public class CustomSerializerConsumer {

    @KafkaListener(
        topics = "test-messages",
        groupId = "custom-serializer-group",
        containerFactory = "customKafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, TestMessage> record, Acknowledgment acknowledgment) {
        String key = record.key();
        TestMessage value = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.info("接收到消息：key={}, partition={}, offset={}, value={}",
            key, partition, offset, value);

        try {
            // 处理消息
            log.info("处理TestMessage: messageId={}, content={}",
                value.getMessageId(), value.getContent());

            // 手动提交offset
            acknowledgment.acknowledge();
            log.info("消息处理完成并提交offset: messageId={}, offset={}",
                value.getMessageId(), offset);
        } catch (Exception e) {
            log.error("处理消息时发生错误: key={}, value={}, error={}",
                key, value, e.getMessage(), e);
        }
    }

}
