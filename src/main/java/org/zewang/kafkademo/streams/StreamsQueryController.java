package org.zewang.kafkademo.streams;


import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 18:55
 */

@RestController
@RequestMapping("/api/streams")
public class StreamsQueryController {

    @Autowired
    private InteractiveQueryService queryService;

    @GetMapping("/click-counts")
    public List<Map<String, Object>> getCurrentClickCounts() {
        List<Map<String, Object>> results = new ArrayList<>();

        // 检查 KafkaStreams 是否已启动
        if (!isKafkaStreamsRunning()) {
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("error", "KafkaStreams 尚未启动，请稍后再试");
            results.add(errorResult);
            return results;
        }

        try {
            // 使用在 ClickStreamProcessor 中定义的常量名称
            ReadOnlyWindowStore<String, Long> store = queryService.getWindowStore(ClickStreamProcessor.CLICK_COUNT_STORE);
            Instant now = Instant.now();
            // 获取最近15秒的数据作为示例
            KeyValueIterator<Windowed<String>, Long> iterator = store.fetchAll(now.minusSeconds(15), now);

            while (iterator.hasNext()) {
                KeyValueIterator<Windowed<String>, Long> rangeIter = iterator;
                Windowed<String> windowedKey = rangeIter.next().key;
                Long value = rangeIter.next().value; // 修复迭代逻辑

                Map<String, Object> result = new HashMap<>();
                result.put("key", windowedKey.key());
                result.put("windowStart", windowedKey.window().startTime().toString());
                result.put("windowEnd", windowedKey.window().endTime().toString());
                result.put("count", value);
                results.add(result);
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    // 可以添加一个查询用户点击率的端点
    @GetMapping("/user-click-rates")
    public List<Map<String, Object>> getUserClickRates() {
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            ReadOnlyWindowStore<String, Long> store = queryService.getWindowStore(ClickStreamProcessor.USER_CLICK_RATE_STORE);
            Instant now = Instant.now();
            KeyValueIterator<Windowed<String>, Long> iterator = store.fetchAll(now.minusSeconds(60), now);

            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, Long> keyValue = iterator.next();
                Windowed<String> windowedKey = keyValue.key;
                Long value = keyValue.value;

                Map<String, Object> result = new HashMap<>();
                result.put("user", windowedKey.key());
                result.put("windowStart", windowedKey.window().startTime().toString());
                result.put("windowEnd", windowedKey.window().endTime().toString());
                result.put("clicks", value);
                results.add(result);
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    private boolean isKafkaStreamsRunning() {
        try {
            return queryService.isKafkaStreamsRunning();
        } catch (Exception e) {
            return false;
        }
    }
}
