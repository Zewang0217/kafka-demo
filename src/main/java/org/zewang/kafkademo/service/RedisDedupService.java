package org.zewang.kafkademo.service;


import java.util.concurrent.RecursiveTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/10/31 21:27
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisDedupService {

    private final RedisTemplate<String, String> redisTemplate;
    private static final String DEDUP_PREFIX = "kafka_msg_dedup:";
    private static final long EXPIRE_TIME = 24 * 60 * 60; // 24 小时过期时间

    // 检查消息是否已处理过
    public boolean isMessageProcessed(String messageId) {
        String key = DEDUP_PREFIX + messageId;
        Boolean hashKey = redisTemplate.hasKey(key);
        return hashKey != null && hashKey;
    }


}
