package org.zewang.kafkademo.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zewang.kafkademo.entity.UserEvent;
import org.zewang.kafkademo.repository.UserEventRepository;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/02 17:07
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConnectTestService {

    private final UserEventRepository userEventRepository;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.connect.url:http://localhost:8083}")
    private String kafkaConnectUrl;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // 检查 Kafka Connect 服务状态
    public boolean checkKafkaConnectStatus() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(kafkaConnectUrl + "/connectors"))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            log.error("检查 Kafka Connect 状态时出错", e);
            return false;
        }
    }

    // 配置 JDBC Source Connector
    public void configureJdbcSourceConnector() throws Exception {
        Map<String, Object> connectorConfig = Map.of(
            "name", "jdbc-source-connector",
            "config", Map.of(
                "connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector",
                "tasks.max", "1",
                "connection.url", "jdbc:mysql://mysql-db:3306/logs_db?user=root&password=root&allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC",
                "table.whitelist", "user_events",
                "mode", "incrementing",
                "incrementing.column.name", "id",
                "topic.prefix", "mysql-",
                "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                "value.converter", "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable", "false"
            )
        );

        String jsonBody = objectMapper.writeValueAsString(connectorConfig); // 将 Map 转换为 JSON

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(kafkaConnectUrl + "/connectors"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody)) // 使用 POST 请求, 将 JSON 数据作为请求体
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() >= 400) {
            throw new RuntimeException("配置 Connector 失败：" + response.body());
        }

        log.info("JDBC Source Connector 配置成功");
    }

    // 插入测试数据
    public void insertTestData() {
        for (int i = 0; i <= 5; i++) {
            UserEvent event = new UserEvent();
            event.setUserId("user" + i);
            event.setEventType(i % 2 == 0 ? "login" : "purchase");
            event.setEventData("{\"event_id\":\"" + UUID.randomUUID() + "\",\"data\":\"test data " + i + "\"}");
            event.setCreatedAt(LocalDateTime.now());

            userEventRepository.save(event);
            log.info("插入测试数据: userId={}", event.getUserId());

        }
    }

    // 获取所有用户事件
    public List<UserEvent> getAllUserEvents() {
        return userEventRepository.findAll();
    }

    // 创建临时消费者，消费指定主题的数据，返回消费到的记录
    public List<ConsumerRecord<String, String>> consumeFromKafkaTopic(String topicName, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.Collections.singletonList(topicName));

            int pollCount = 0;
            while (records.size() < expectedCount && pollCount < 10) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    records.add(record);
                }
                pollCount++;
            }
        } catch (Exception e) {
            log.error("消费 Kafka 主题时出错", e);
        }

        return records;
    }

    /**
     * 删除 JDBC Source Connector
     */
    public void deleteJdbcSourceConnector() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(kafkaConnectUrl + "/connectors/jdbc-source-connector"))
            .DELETE()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() >= 400 && response.statusCode() != 404) {
            throw new RuntimeException("删除 Connector 失败：" + response.body());
        }

        log.info("JDBC Source Connector 删除成功或不存在");
    }
}
