package org.zewang.kafkademo.config;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.web.ServerProperties.Netty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 19:30
 */

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // 配置 KafkaAdmin
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    // 创建 Topic - kafka-demo
    @Bean
    public NewTopic kafkaDemoTopic() {
        return new NewTopic("kafka-demo", 3, (short) 1); // 主题名，分区数，副本因子
    }

    // 创建 Topic - test-topic 用于测试不同 acks 配置
    @Bean
    public NewTopic testTopic() {
        return new NewTopic("test-topic", 3, (short) 1);
    }

    // 配置acks=0的KafkaTemplate
    @Bean("kafkaTemplateAcks0")
    public KafkaTemplate<String, String> kafkaTemplateAcks0() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.ACKS_CONFIG, "0"); // 不等待服务器确认
        configs.put(ProducerConfig.RETRIES_CONFIG, 0); // 不重试

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configs);
        return new KafkaTemplate<>(producerFactory);
    }

    // 配置acks=1的KafkaTemplate
    @Bean("kafkaTemplateAcks1")
    public KafkaTemplate<String, String> kafkaTemplateAcks1() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.ACKS_CONFIG, "1"); // 等待leader确认

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configs);
        return new KafkaTemplate<>(producerFactory);
    }

    // 配置acks=all的KafkaTemplate
    @Bean("kafkaTemplateAcksAll")
    public KafkaTemplate<String, String> kafkaTemplateAcksAll() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // 启用幂等性

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configs);
        return new KafkaTemplate<>(producerFactory);
    }


}
