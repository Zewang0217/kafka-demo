package org.zewang.kafkademo.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.zewang.kafkademo.service.PerformanceTestService;
import org.zewang.kafkademo.service.admin.TopicManagementService;

// PerformanceTestController.java
@RestController
@RequestMapping("/api/performance")
@RequiredArgsConstructor
@Slf4j
public class PerformanceTestController {

    private final PerformanceTestService performanceTestService;
    private final TopicManagementService topicManagementService;

    @Autowired
    @Qualifier("optimizedKafkaTemplate")
    private KafkaTemplate<String, String> optimizedKafkaTemplate;

    @Autowired
    @Qualifier("kafkaTemplateAcks0")
    private KafkaTemplate<String, String> kafkaTemplateAcks0;

    @Autowired
    @Qualifier("kafkaTemplateAcks1")
    private KafkaTemplate<String, String> kafkaTemplateAcks1;

    @Autowired
    @Qualifier("kafkaTemplateAcksAll")
    private KafkaTemplate<String, String> kafkaTemplateAcksAll;

    @PostMapping("/test")
    public ResponseEntity<Map<String, Object>> runPerformanceTest(
        @RequestParam(defaultValue = "10000") int messageCount,
        @RequestParam(defaultValue = "optimized") String templateType,
        @RequestParam(defaultValue = "performance-test-topic") String topic) {

        // 确保topic存在
        topicManagementService.createTopic(topic, 6, (short) 1);

        KafkaTemplate<String, String> template = getKafkaTemplate(templateType);
        String testName = "performance-test-" + templateType;

        long startTime = System.currentTimeMillis();
        performanceTestService.startTest(testName);

        // 发送消息
        for (int i = 0; i < messageCount; i++) {
            String key = "key-" + i;
            String message = generateMessage(i);
            template.send(topic, key, message);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double tps = (double) messageCount / duration * 1000;

        Map<String, Object> result = new HashMap<>();
        result.put("messageCount", messageCount);
        result.put("durationMs", duration);
        result.put("tps", String.format("%.2f", tps));
        result.put("templateType", templateType);

        return ResponseEntity.ok(result);
    }

    private KafkaTemplate<String, String> getKafkaTemplate(String templateType) {
        switch (templateType) {
            case "acks0":
                return kafkaTemplateAcks0;
            case "acks1":
                return kafkaTemplateAcks1;
            case "acksAll":
                return kafkaTemplateAcksAll;
            case "optimized":
            default:
                return optimizedKafkaTemplate;
        }
    }

    private String generateMessage(int index) {
        return "Performance test message #" + index +
            " with content: " + UUID.randomUUID().toString() +
            " timestamp: " + System.currentTimeMillis();
    }
}
