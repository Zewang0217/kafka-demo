package org.zewang.kafkademo.config.serialize;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/01 19:22
 */

@RequiredArgsConstructor
public class CustomJsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper(); // Jackson对象映射器
    private Class<T> clazz; // 泛型类

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // 使用JavaTimeModule以支持LocalDateTime等时间类型
        objectMapper.registerModule(new JavaTimeModule());

        // 可以从配置中获取类信息
        if (clazz == null && configs.containsKey("serializedClass")) {
            try {
                clazz = (Class<T>) Class.forName((String) configs.get("serializedClass"));
            } catch (ClassNotFoundException e) {
                throw new SerializationException("无法加载序列化类", e);
            }
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("无法将数据转换为JSON", e);
        }
    }

    @Override
    public void close() {
        // 清理资源
    }
}
