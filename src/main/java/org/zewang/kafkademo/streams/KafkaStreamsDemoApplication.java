package org.zewang.kafkademo.streams;


import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 流处理实战 demo
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 18:39
 */

@EnableKafkaStreams
@SpringBootApplication(scanBasePackages = {"org.zewang.kafkademo", "org.zewang.kafkademo.streams"})
public class KafkaStreamsDemoApplication {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsDemoApplication.class, args);
    }

    @Bean
    public StreamsBuilderFactoryBeanCustomizer customizer() {
        return factoryBean -> {
            Properties props = factoryBean.getStreamsConfiguration();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        };
    }
}
