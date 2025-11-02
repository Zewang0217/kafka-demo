// TestMessageController.java
package org.zewang.kafkademo.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.zewang.kafkademo.entity.TestMessage;
import org.zewang.kafkademo.service.TestMessageService;
import java.util.List;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 测试消息控制器，提供REST API接口用于查看和管理测试消息
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/1 10:40
 */
@Slf4j
@RestController
@RequestMapping("/api/test-messages")
@RequiredArgsConstructor
public class TestMessageController {

    private final TestMessageService testMessageService;

    /**
     * 获取所有测试消息
     * @return 消息列表
     */
    @GetMapping
    public List<TestMessage> getAllMessages() {
        log.info("获取所有测试消息");
        return testMessageService.getAllMessages();
    }

    /**
     * 根据消息ID查找消息
     * @param messageId 消息唯一标识
     * @return 消息对象
     */
    @GetMapping("/{messageId}")
    public TestMessage getMessageById(@PathVariable String messageId) {
        log.info("查找消息: messageId={}", messageId);
        return testMessageService.findByMessageId(messageId)
            .orElseThrow(() -> new RuntimeException("未找到消息: " + messageId));
    }

    /**
     * 删除所有测试消息
     * @return 删除结果
     */
    @DeleteMapping
    public String deleteAllMessages() {
        long count = testMessageService.deleteAllMessages();
        log.info("删除所有测试消息，共删除 {} 条记录", count);
        return String.format("成功删除 %d 条消息记录", count);
    }
}
