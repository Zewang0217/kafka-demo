package org.zewang.kafkademo.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 对应 MySQL 数据库中的日志表
 *               使用 Lombok 简化代码
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/29 13:31
 */

@Entity
@Table(name = "web_log_entries",
uniqueConstraints = {
    @UniqueConstraint(columnNames = {"idAddress", "timestamp", "method", "path"})
})
@Data
@NoArgsConstructor // 自动生成无参构造函数
@AllArgsConstructor // 自动生成全参构造函数
public class LogEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private LocalDateTime timestamp; // 日志记录时间

    @Column(length = 50)
    private String idAddress; // 客户端IP地址

    @Column(length = 10)
    private String method; // HTTP 方法

    @Column(columnDefinition = "TEXT")
    private String path; // 请求路径（URL）

    private int statusCode; // HTTP 状态码

    private long responseTimeMs; // 响应时间（毫秒）

    @Column(columnDefinition = "TEXT")
    private String userAgent; // 用户代理

    @Column(columnDefinition = "TEXT")
    private String originalMessage; // 原始的、未解析的Kafka消息体

    // 字段的解析和结构化逻辑在 Consumer Service 中实现
}
