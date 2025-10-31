package org.zewang.kafkademo.service.admin;


import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties.Netty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 20:05
 */

@Slf4j
@Service
public class TopicManagementService {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    // 创建新的 Topic
    public boolean createTopic(String topicName, int partitions, short replicationFactor) {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get(); // 等待创建完成
            log.info("成功创建 Topic：{}", topicName);
            return true;
        } catch (Exception e) {
            log.error("创建 Topic 失败：{}", e.getMessage());
            return false;
        } finally {
            adminClient.close();
        }
    }

    // 删除 Topic
    public boolean deleteTopic(String topicName) {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topicName));
            result.all().get(); // 等待删除完成
            log.info("成功删除 Topic: {}", topicName);
            return true;
        } catch (Exception e) {
            log.error("删除 Topic 失败: {}", e.getMessage());
            return false;
        } finally {
            adminClient.close();
        }
    }
}
