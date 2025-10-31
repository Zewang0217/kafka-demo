package org.zewang.kafkademo.dto.rest;


import lombok.Data;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 20:18
 */

@Data
public class CreateTopicRequest {
    private String topicName;
    private int partitions = 3;
    private short replicationFactor = 1;
}
