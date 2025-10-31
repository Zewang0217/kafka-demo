// TestMessage.java
package org.zewang.kafkademo.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 测试消息实体类，用于演示Kafka消息处理和去重
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/1 10:00
 */
@Entity
@Table(name = "test_messages")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 消息唯一标识，用于去重
     */
    @Column(unique = true, nullable = false)
    private String messageId;

    /**
     * 消息内容
     */
    @Column(nullable = false)
    private String content;

    /**
     * 消息发送者
     */
    private String sender;

    /**
     * 消息接收时间
     */
    @Column(nullable = false)
    private LocalDateTime receivedTime;

    /**
     * 处理状态
     */
    private String status = "PENDING";

    /**
     * 处理次数
     */
    private Integer processCount = 0;
}
