package org.zewang.kafkademo.consumer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.zewang.kafkademo.model.LogEntry;
import org.zewang.kafkademo.repository.LogEntryRepository;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/30 18:36
 */

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaLogConsumer {

    private final LogEntryRepository logEntryRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // 假设的常见访问日志格式（结合 Filebeat/Logstash 的通用输出）
    // 这是一个用于匹配典型 Apache/Nginx combined log format 的正则表达式
    // 示例日志片段: 192.168.1.10 - - [21/Jun/2024:10:00:00 +0800] "GET /api/status HTTP/1.1" 200 125 5ms "Mozilla/5.0"
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) - - " +   // 1. IP Address
            "\\[(.+?)\\] " +                                     // 2. Timestamp (原始格式)
            "\"(GET|POST|PUT|DELETE|HEAD) (.+?) HTTP/1\\.[01]\" " + // 3. Method, 4. Path
            "(\\d{3}) " +                                        // 5. Status Code
            "(\\d+)ms " +                                        // 6. Response Time (这里假设您的日志包含响应时间，例如 5ms)
            "\"(.+?)\""                                          // 7. User Agent
    );

    // 常用日期格式：例如 21/Jun/2024:10:00:00 +0800。注意：Filebeat/Kafka 可能会改变日期格式，请根据实际情况调整！
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

    /**
     * Kafka 监听器：消费指定主题中的日志信息
     */
//    @KafkaListener(topics = "web-logs", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void listen(String jsonMessage) {
        if (jsonMessage == null || jsonMessage.trim().isEmpty()) {
            log.warn("Received empty or null Kafka message. Skipping.");
            return;
        }

        String rawLogMessage = null;

        try {
            // -------------------- 关键修正点 1: JSON 解析 --------------------
            JsonNode rootNode = objectMapper.readTree(jsonMessage);
            JsonNode messageNode = rootNode.get("message");

            if (messageNode != null && messageNode.isTextual()) {
                rawLogMessage = messageNode.asText();
            } else {
                // 如果没有找到 'message' 字段，或者不是文本，可能就是 Filebeat 自己的元数据或旧的测试消息。
                rawLogMessage = jsonMessage; // 尝试用原始消息进行解析（以防万一）
            }

            log.info("提取原始日志信息: {}", rawLogMessage);

            // -------------------- 关键修正点 2: 原始日志解析 --------------------
            LogEntry logEntry = parseLogMessage(rawLogMessage);

            // 保存到 MySQL 数据库
            LogEntry savedEntry = logEntryRepository.save(logEntry);
            log.debug("成功保存 LogEntry ID: {}", savedEntry.getId());

        } catch (IOException e) {
            // JSON 解析失败 (可能不是Filebeat的JSON格式)
            log.error("JSON解析错误，无法从消息中提取原始日志: {} | Error: {}", jsonMessage, e.getMessage());
        } catch (IllegalArgumentException e) {
            // 日志内容正则表达式匹配失败
            log.warn("解析日志信息时出错：日志信息与期望格式不匹配。原始消息: {}", rawLogMessage, e);
        } catch (Exception e) {
            // 数据库或其他异常
            log.error("数据库持久化失败: {} | Error: {}", rawLogMessage, e.getMessage(), e);
        }
    }

    /**
     * 解析原始日志信息字符串，并结构化为 LogEntry 对象
     */
    private LogEntry parseLogMessage(String rawLogMessage) throws IllegalArgumentException {
        Matcher matcher = LOG_PATTERN.matcher(rawLogMessage);

        if (!matcher.find()) {
            throw new IllegalArgumentException("日志信息与期望格式不匹配");
        }

        LogEntry entry = new LogEntry();
        entry.setOriginalMessage(rawLogMessage);

        try {
            // IP Address
            entry.setIdAddress(matcher.group(1));

            String rawTimestamp = matcher.group(2).replace(":", " ").replaceFirst(" ", ":");
            // 假设原始格式是 [21/Jun/2024:10:00:00 +0800] -> 21/Jun/2024:10:00:00 +0800
            try {
                LocalDateTime timestamp = LocalDateTime.parse(matcher.group(2), DATE_TIME_FORMATTER);
                entry.setTimestamp(timestamp);
            } catch (DateTimeParseException e) {
                log.warn("用默认格式器解析时间失败：{}", rawTimestamp);
                entry.setTimestamp(LocalDateTime.now());
            }

            // 3. HTTP Method
            entry.setMethod(matcher.group(3));

            // 4. Request Path
            entry.setPath(matcher.group(4));

            // 5. Status Code
            entry.setStatusCode(Integer.parseInt(matcher.group(5)));

            // 6. Response Time (移除 'ms' 后解析)
            String responseTimeStr = matcher.group(6);
            entry.setResponseTimeMs(Long.parseLong(responseTimeStr));

            // 7. User Agent
            entry.setUserAgent(matcher.group(7));

            return entry;
        } catch (Exception e) {
            throw new IllegalArgumentException("日志信息解析失败：" + e.getMessage());
        }
    }

}
