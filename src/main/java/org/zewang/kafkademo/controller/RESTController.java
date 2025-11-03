package org.zewang.kafkademo.controller;


import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.zewang.kafkademo.dto.rest.CreateTopicRequest;
import org.zewang.kafkademo.dto.rest.KafkaMessageRequest;
import org.zewang.kafkademo.service.KafkaMessageService;
import org.zewang.kafkademo.service.RedisDedupService;
import org.zewang.kafkademo.service.admin.TopicManagementService;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: REST 控制器，提供不同acks级别的消息发送接口
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 19:46
 */

@Slf4j
@RestController
@RequestMapping("/api/kafka")
public class RESTController {

    @Autowired
    private KafkaMessageService kafkaMessageService;
    @Autowired
    private TopicManagementService topicManagementService;
    @Autowired
    private RedisDedupService redisDedupService;

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

    // 发送消息并指定acks级别
    @PostMapping("/send")
    public String sendMessage(@RequestBody KafkaMessageRequest request) {
        try {
            switch (request.getAcks()) {
                case "0":
                    kafkaMessageService.sendWithAcks0(request.getTopic(), request.getKey(), request.getMessage());
                    break;
                case "1":
                    kafkaMessageService.sendWithAcks1(request.getTopic(), request.getKey(), request.getMessage());
                    break;
                case "all":
                    kafkaMessageService.sendWithAcksAll(request.getTopic(), request.getKey(), request.getMessage());
                    break;
                default:
                    return "非法参数： acks等级。使用 0、1、all";
            }
            return "消息发送成功，acks=" + request.getAcks();
        } catch (Exception e) {
            log.error("消息发送失败：{}", e.getMessage());
            return "消息发送失败：" + e.getMessage();
        }
    }

    // 创建 Topic
    @PostMapping("/topics")
    public String createTopic(@RequestBody CreateTopicRequest request) {
        // 验证 topicName 是否合法
        if (request.getTopicName() == null || request.getTopicName().trim().isEmpty()) {
            return "Topic 名称不能为空";
        }

        // 验证 topicName 格式是否符合 Kafka 要求
        if (!isValidTopicName(request.getTopicName())) {
            return "Topic 名称格式不正确，只能包含字母、数字、点(.)、下划线(_)和连字符(-)";
        }

        boolean success = topicManagementService.createTopic(
            request.getTopicName(),
            request.getPartitions(),
            request.getReplicationFactor()
        );
        if (success) {
            return "Topic '" + request.getTopicName() + "' 创建成功";
        } else {
            return "Topic '" + request.getTopicName() + "' 创建失败";
        }
    }

    // 删除 Topic
    @DeleteMapping("/topics/{topicName}")
    public String deleteTopic(@PathVariable String topicName) {
        boolean success = topicManagementService.deleteTopic(topicName);
        if (success) {
            return "Topic '" + topicName + "' 删除成功";
        } else {
            return "Topic '" + topicName + "' 删除失败";
        }
    }


    // 验证 Topic 名称是否符合 Kafka 要求
    private boolean isValidTopicName(String topicName) {
        // Kafka Topic 名称规则：
        // 1. 长度在 1-249 个字符之间
        // 2. 只能包含 ASCII 字母数字、.、_ 和 -
        // 3. 不能以点(.)开头或结尾
        // 4. 不能包含连续的点(..)

        if (topicName == null || topicName.length() == 0 || topicName.length() > 249) {
            return false;
        }

        // 检查字符是否合法
        if (!topicName.matches("[a-zA-Z0-9._-]+")) {
            return false;
        }

        // 不能以点开头或结尾
        if (topicName.startsWith(".") || topicName.endsWith(".")) {
            return false;
        }

        // 不能包含连续的点
        if (topicName.contains("..")) {
            return false;
        }

        return true;
    }

    @PostMapping("/redis/clean")
    public String cleanRedis(@RequestParam String redisKey) {
        redisDedupService.clearAll();
        return "Redis 数据清理完成";
    }

    // 性能测试
    @PostMapping("/performance-test")
    public String perfomanceTest(@RequestParam(defaultValue = "1000") int messageCount,
        @RequestParam(defaultValue = "optimized") String templateType) {
        long startTime = System.currentTimeMillis();

        KafkaTemplate<String, String> template;
        switch (templateType) {
            case "acks0":
                template = kafkaTemplateAcks0;
                break;
            case "acks1":
                template = kafkaTemplateAcks1;
                break;
            case "acksAll":
                template = kafkaTemplateAcksAll;
                break;
            case "optimized":
            default:
                template = optimizedKafkaTemplate; // 使用上面定义的优化模板
                break;
        }

        String topic = "performance-test-topic";
        topicManagementService.createTopic(topic, 3, (short) 1);

        // 发送批量消息
        for (int i = 0; i < messageCount; i++) {
            String key = "key-" + i;
            String message = "Performance test message #" + i + " with content: " + UUID.randomUUID().toString();
            template.send(topic, key, message);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        return String.format("发送 %d 条消息耗时 %d ms，平均 %.2f ms/条",
            messageCount, duration, (double)duration/messageCount);
    }
}
