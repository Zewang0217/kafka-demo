package org.zewang.kafkademo.config;


import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.zewang.kafkademo.config.serialize.CustomJsonDeserializer;
import org.zewang.kafkademo.config.serialize.CustomJsonSerializer;
import org.zewang.kafkademo.entity.TestMessage;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: Kafka 配置类
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

    @Bean
    public NewTopic testMessagesTopic() {
        return new NewTopic("test-messages", 3, (short) 1);
    }

    // 配置消费者工厂
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-message-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // 配置消费者工厂 - 自定义反序列化
    @Bean
    public ConsumerFactory<String, TestMessage> customConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-serializer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomJsonDeserializer.class);
        props.put("serializedClass", TestMessage.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // 优化的消费者工厂
    @Bean
    public ConsumerFactory<String, String> optimizedConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "optimized-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Consumer Batching 优化
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // 最小拉取 1KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // 最大等待 500ms
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // 每次 poll 最多 500 条记录

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // 创建 KafkaListenerContainerFactory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // 创建 KafkaListenerContainerFactory - 自定义反序列化
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TestMessage> customKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TestMessage> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(customConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // 配置使用自定义序列化器的生产者工厂
    @Bean
    public ProducerFactory<String, TestMessage> customProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomJsonSerializer.class);
        props.put("serializedClass", "org.zewang.kafkademo.entity.TestMessage");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, TestMessage> customKafkaTemplate() {
        return new KafkaTemplate<>(customProducerFactory());
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

    // 优化 Producer 配置的template
    @Bean("optimizedKafkaTemplate")
    public KafkaTemplate<String, String> optimizedKafkaTemplate() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.ACKS_CONFIG, "1");

        // Batching 优化配置
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16KB 批处理大小
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 5); // 5ms 延迟以增加批处理机会
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 使用 Snappy 压缩
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB 缓冲区

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configs);
        return new KafkaTemplate<>(producerFactory);
    }

    // 错误处理策略
    @Bean
    public DefaultErrorHandler errorHandler() {
        // 配置重试策略
        FixedBackOff backOff = new FixedBackOff(2000, 3); // 间隔2秒，最多重试3次
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);

        // 对于特定异常，可以选择不重试
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.addNotRetryableExceptions(JsonProcessingException.class);

        return errorHandler;
    }


}
