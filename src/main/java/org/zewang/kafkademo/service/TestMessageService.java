// TestMessageService.java
package org.zewang.kafkademo.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.zewang.kafkademo.entity.TestMessage;
import org.zewang.kafkademo.repository.TestMessageRepository;
import java.util.List;
import java.util.Optional;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 测试消息服务类，提供消息的业务处理方法
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/1 10:30
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TestMessageService {

    private final TestMessageRepository testMessageRepository;

    /**
     * 保存消息到数据库
     * @param testMessage 测试消息对象
     * @return 保存后的消息对象
     */
    public TestMessage saveMessage(TestMessage testMessage) {
        try {
            TestMessage savedMessage = testMessageRepository.save(testMessage);
            log.info("消息保存成功: id={}, messageId={}", savedMessage.getId(), savedMessage.getMessageId());
            return savedMessage;
        } catch (Exception e) {
            log.error("保存消息失败: messageId={}, error={}", testMessage.getMessageId(), e.getMessage());
            throw new RuntimeException("保存消息到数据库失败", e);
        }
    }

    /**
     * 根据消息ID查找消息
     * @param messageId 消息唯一标识
     * @return 消息对象
     */
    public Optional<TestMessage> findByMessageId(String messageId) {
        return testMessageRepository.findByMessageId(messageId);
    }

    /**
     * 获取所有消息
     * @return 消息列表
     */
    public List<TestMessage> getAllMessages() {
        return testMessageRepository.findAll();
    }

    /**
     * 删除所有消息（用于测试清理）
     * @return 删除的记录数
     */
    public long deleteAllMessages() {
        long count = testMessageRepository.count();
        testMessageRepository.deleteAll();
        return count;
    }
}
