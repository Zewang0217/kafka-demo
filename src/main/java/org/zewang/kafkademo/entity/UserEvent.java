package org.zewang.kafkademo.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/02 17:00
 */

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "user_events")
public class UserEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY) // 主键生成策略: 自增
    private Long id;

    @Column(name = "user_id")
    private String userId;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "event_data", columnDefinition = "TEXT")
    private String eventData;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}
