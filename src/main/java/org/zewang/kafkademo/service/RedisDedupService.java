package org.zewang.kafkademo.service;


import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
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
    private static final String DEDUP_PREFIX = "kafka_msg_dedup:"; // Redis键前缀
    private static final long EXPIRE_TIME = 24 * 60 * 60; // 24 小时过期时间

    // 检查消息是否已处理过
    public boolean isMessageProcessed(String messageId) {
        String key = DEDUP_PREFIX + messageId;
        Boolean hasKey = redisTemplate.hasKey(key);
        boolean processed = hasKey != null && hasKey;
        if (processed) {
            log.debug("消息已处理过: {}", messageId);
        }
        return processed;
    }

    // 标记消息已处理
    public void markMessageAsProcessed(String messageId) {
        String key = DEDUP_PREFIX + messageId;
        redisTemplate.opsForValue().set(key, "1", EXPIRE_TIME, TimeUnit.SECONDS);
        log.debug("标记消息已处理: {}", messageId);
    }

    // 清除缓存
    // RedisDedupService.java
    public void clearAll() {
        redisTemplate.getConnectionFactory().getConnection().flushAll();
        log.info("已清理所有Redis数据");
    }

}
