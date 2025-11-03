package org.zewang.kafkademo.service;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.View;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 13:11
 */

@Service
public class KafkaMetricsService {

    private final MeterRegistry meterRegistry;
    private final Timer producerTimer;
    private final Counter errorCounter;

    public KafkaMetricsService(MeterRegistry meterRegistry, View error) {
        this.meterRegistry = meterRegistry;
        this.producerTimer = Timer.builder("kafka.producer.send.time")
            .description("kafka消息发送时间")
            .register(meterRegistry);
        this.errorCounter = Counter.builder("kafka.producer.errors")
            .description("Kafka消息发送错误计数")
            .register(meterRegistry);
    }

    public void recordSendTime(long timeInMillis) {
        producerTimer.record(timeInMillis, TimeUnit.MILLISECONDS);
    }

    public void recordError() {
        errorCounter.increment();
    }
}
