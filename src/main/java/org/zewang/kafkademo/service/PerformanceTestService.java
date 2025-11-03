// PerformanceTestService.java
package org.zewang.kafkademo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class PerformanceTestService {

    private final ConcurrentHashMap<String, AtomicLong> messageCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> startTimes = new ConcurrentHashMap<>();

    public void startTest(String testName) {
        messageCounters.put(testName, new AtomicLong(0));
        startTimes.put(testName, System.currentTimeMillis());
        log.info("开始性能测试: {}", testName);
    }

    public void recordMessage(String testName) {
        AtomicLong counter = messageCounters.get(testName);
        if (counter != null) {
            long count = counter.incrementAndGet();
            if (count % 1000 == 0) {
                Long startTime = startTimes.get(testName);
                if (startTime != null) {
                    long duration = System.currentTimeMillis() - startTime;
                    double tps = (double) count / duration * 1000;
                    log.info("测试 {} 已处理 {} 条消息，耗时 {} ms，TPS: {:.2f}",
                        testName, count, duration, tps);
                }
            }
        }
    }

    public void endTest(String testName) {
        AtomicLong counter = messageCounters.get(testName);
        Long startTime = startTimes.get(testName);
        if (counter != null && startTime != null) {
            long count = counter.get();
            long duration = System.currentTimeMillis() - startTime;
            double tps = (double) count / duration * 1000;
            log.info("测试 {} 完成: 处理 {} 条消息，耗时 {} ms，平均TPS: {:.2f}",
                testName, count, duration, tps);
        }
    }
}
