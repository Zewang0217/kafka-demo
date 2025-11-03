package org.zewang.kafkademo.config.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomJsonDeserializer<T> implements Deserializer<T>  {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> clazz;

    public CustomJsonDeserializer() {
        // 注册JavaTimeModule以支持LocalDateTime等时间类型
        objectMapper.registerModule(new JavaTimeModule());
    }

    public CustomJsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
        // 注册JavaTimeModule以支持LocalDateTime等时间类型
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (clazz == null && configs.containsKey("serializedClass")) {
            try {
                clazz = (Class<T>) Class.forName((String) configs.get("serializedClass"));
            } catch (ClassNotFoundException e) {
                throw new SerializationException("无法加载反序列化类", e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new SerializationException("反序列化对象时出错: " + new String(data), e);
        }
    }

    @Override
    public void close() {
        // 清理资源
    }
}
