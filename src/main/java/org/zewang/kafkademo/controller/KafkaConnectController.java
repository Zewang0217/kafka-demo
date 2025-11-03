package org.zewang.kafkademo.controller;


import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zewang.kafkademo.entity.UserEvent;
import org.zewang.kafkademo.service.KafkaConnectTestService;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 测试 KafkaConnect 的控制器
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/02 17:06
 */

@Slf4j
@RestController
@RequestMapping("/api/kafka-connect")
@RequiredArgsConstructor
public class KafkaConnectController {
    private final KafkaConnectTestService kafkaConnectTestService;

    // 插入测试数据到 user_events 表
    @PostMapping("/user-events")
    public ResponseEntity<String> insertTestData() {
        try {
            kafkaConnectTestService.insertTestData();
            return ResponseEntity.ok("测试数据插入成功");
        } catch (Exception e) {
            log.error("插入测试数据失败", e);
            return ResponseEntity.status(500).body("插入测试数据失败: " + e.getMessage());
        }
    }

    // 获取所有用户事件数据
    @GetMapping("user-events")
    public ResponseEntity<List<UserEvent>> getAllUserEvents() {
        try {
            List<UserEvent> events = kafkaConnectTestService.getAllUserEvents();
            return ResponseEntity.ok(events);
        } catch (Exception e) {
            log.error("获取用户事件数据失败", e);
            return ResponseEntity.status(500).build();
        }
    }

    // 验证 Kafka Connect 配置
    @GetMapping("/status")
    public ResponseEntity<String> checkConnectStatus() {
        try {
            boolean isAvailable = kafkaConnectTestService.checkKafkaConnectStatus();
            if (isAvailable) {
                return ResponseEntity.ok("Kafka Connect 服务可用");
            } else {
                return ResponseEntity.status(500).body("Kafka Connect 服务不可用");
            }
        } catch (Exception e) {
            log.error("检查 Kafka Connect 状态失败", e);
            return ResponseEntity.status(500).body("检查 Kafka Connect 状态失败: " + e.getMessage());
        }
    }

    /**
     * 配置 JDBC Source Connector
     */
    @PostMapping("/connector")
    public ResponseEntity<String> configureJdbcConnector() {
        try {
            kafkaConnectTestService.configureJdbcSourceConnector();
            return ResponseEntity.ok("JDBC Source Connector 配置成功");
        } catch (Exception e) {
            log.error("配置 JDBC Source Connector 失败", e);
            return ResponseEntity.status(500).body("配置失败: " + e.getMessage());
        }
    }

    // 测试 JDBC Source Connector：在数据库插入数据，观察主题中是否有数据
    @GetMapping("/test-jdbc-connector")
    public ResponseEntity<String> testJdbcConnector() {
        try {

            LocalDateTime startTime = LocalDateTime.now();

            int testDataCount = 6;
            kafkaConnectTestService.insertTestData();

            Thread.sleep(5000);

            // 消费 Kafka 主题验证数据同步
            List<ConsumerRecord<String, String>> records = kafkaConnectTestService
                .consumeFromKafkaTopic("mysql-user_events", testDataCount);

            if (records.size() >= testDataCount) {
                return ResponseEntity.ok("JDBC Source Connector 测试成功，Kafka 主题收到 " + records.size() + " 条消息");
            } else {
                return ResponseEntity.status(500).body("数据同步不完整，期望 " + testDataCount + " 条，实际收到 " + records.size() + " 条");
            }
        } catch (Exception e) {
            log.error("测试 JDBC Source Connector 失败", e);
            return ResponseEntity.status(500).body("测试失败");
        }
    }

    /**
     * 删除 JDBC Source Connector
     */
    @PostMapping("/connector/delete")
    public ResponseEntity<String> deleteJdbcConnector() {
        try {
            kafkaConnectTestService.deleteJdbcSourceConnector();
            return ResponseEntity.ok("JDBC Source Connector 删除成功");
        } catch (Exception e) {
            log.error("删除 JDBC Source Connector 失败", e);
            return ResponseEntity.status(500).body("删除失败: " + e.getMessage());
        }
    }
}
