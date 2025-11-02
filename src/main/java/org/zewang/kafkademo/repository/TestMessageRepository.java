// TestMessageRepository.java
package org.zewang.kafkademo.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.zewang.kafkademo.entity.TestMessage;
import java.util.Optional;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TestMessage实体的数据库操作接口
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/1 10:05
 */
@Repository
public interface TestMessageRepository extends JpaRepository<TestMessage, Long> {

    /**
     * 根据消息ID查找消息
     * @param messageId 消息唯一标识
     * @return TestMessage对象
     */
    Optional<TestMessage> findByMessageId(String messageId);
}
