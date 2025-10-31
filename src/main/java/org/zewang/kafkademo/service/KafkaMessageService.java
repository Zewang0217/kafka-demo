package org.zewang.kafkademo.service;


import jakarta.websocket.SendResult;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 19:36
 */

@Service
@Slf4j
public class KafkaMessageService {

    @Autowired
    @Qualifier("kafkaTemplateAcks0")
    private KafkaTemplate<String, String> kafkaTemplateAcks0;

    @Autowired
    @Qualifier("kafkaTemplateAcks1")
    private KafkaTemplate<String, String> kafkaTemplateAcks1;

    @Autowired
    @Qualifier("kafkaTemplateAcksAll")
    private KafkaTemplate<String, String> kafkaTemplateAcksAll;

    // 异步发送消息 - acks=0
    public void sendWithAcks0(String topic, String key, String message) {

        kafkaTemplateAcks0.send(topic, key, message).whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println(
                    "Sent message with acks=0 [key: " + key + ", value: " + message + "]");
            } else {
                log.error("Unable to send message with acks=0: " + ex.getMessage());
            }
        });
    }

    // 异步发送消息 - acks=1
    public void sendWithAcks1(String topic, String key, String message) {
        kafkaTemplateAcks1.send(topic, key, message).whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message with acks=1 [key: " + key + ", value: " + message +
                    "] partition: " + result.getRecordMetadata().partition() +
                    " offset: " + result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message with acks=1: " + ex.getMessage());
            }
        });
    }

    // 异步发送消息 - acks=all
    public void sendWithAcksAll(String topic, String key, String message) {
        kafkaTemplateAcksAll.send(topic, key, message).whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message with acks=all [key: " + key + ", value: " + message +
                    "] partition: " + result.getRecordMetadata().partition() +
                    " offset: " + result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message with acks=all: " + ex.getMessage());
            }
        });
    }
}
