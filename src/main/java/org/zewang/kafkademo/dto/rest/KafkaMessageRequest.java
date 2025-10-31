package org.zewang.kafkademo.dto.rest;


import lombok.Data;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 19:49
 */

@Data
public class KafkaMessageRequest {
    public String topic;
    public String key;
    public String message;
    public String acks = "1"; // "0" "1" "all"
}
