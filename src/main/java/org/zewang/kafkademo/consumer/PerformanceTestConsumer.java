package org.zewang.kafkademo.consumer;


import jakarta.persistence.Column;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hibernate.dialect.function.CastStrEmulation;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.zewang.kafkademo.service.PerformanceTestService;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 性能调优测试
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 10:37
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class PerformanceTestConsumer {

    private final PerformanceTestService performanceTestService;
    private static final String TEST_NAME =  "performance-test";

    private final AtomicLong messageCount = new AtomicLong(0);
    private volatile long startTime; // volatile 修饰的变量，多线程下可见

    @KafkaListener(topics = "performance-test-topic",
        groupId = "performance-test-group",
        containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record) {
        performanceTestService.recordMessage(TEST_NAME);

    }

}
