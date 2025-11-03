package org.zewang.kafkademo.streams;


import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: TODO (这里用一句话描述这个类的作用)
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 18:43
 */

@Configuration
public class ClickStreamProcessor {

    // 定义输入 Topic 的名称
    public static final String CLICK_EVENTS_TOPIC = "user-click-events";
    // 定义滚动窗口统计结果输出 Topic 的名称
    public static final String CLICK_COUNT_OUTPUT = "click-count-output";
    // 定义滑动窗口统计结果输出 Topic 的名称
    public static final String USER_CLICK_RATE_OUTPUT = "user-click-rate-output";

    // 定义状态存储的名称常量
    public static final String CLICK_COUNT_STORE = "click-count-store";
    public static final String USER_CLICK_RATE_STORE = "user-click-rate-store";


    /**
     * 定义并注册一个 KStream Bean。
     * Spring Kafka Streams 会自动使用 StreamsBuilder 来调用这个方法，
     * 并将方法返回的 KStream 拓扑添加到最终的处理拓扑中。
     * @param streamsBuilder 由 Spring 自动注入的 StreamsBuilder 实例
     * @return KStream 实例，代表处理逻辑的起点
     */
    @Bean
    public KStream<String, String> KStream(StreamsBuilder streamsBuilder) {
        //    使用 StreamsBuilder 从指定的 Kafka Topic 创建一个 KStream 实例。
        //    这个 KStream 代表了从 "user-click-events" Topic 持续不断流入的数据流。
        //    数据的 Key 和 Value 都是 String 类型。
        KStream<String, String> sourceStream = streamsBuilder.stream(CLICK_EVENTS_TOPIC);

        // 1. 实时统计每10秒内的消息数量 （Tumbling Window）
        processTumblingWindow(sourceStream);

        // 2. 计数用户过去5分钟内的平均点击率 （Hopping Window）
        processHoppingWindow(sourceStream);

        // 3. 返回 sourceStream。虽然这里返回了，但实际的处理逻辑已经在上面的
        //    processTumblingWindow 和 processHoppingWindow 方法中通过拓扑连接完成。
        //    返回值主要是为了让 Spring 管理这个 Bean。
        return sourceStream;
    }

    /**
     * 使用 Tumbling Window（滚动窗口）实时统计每10秒内的消息总数量
     * Tumbling Window: 固定大小，不重叠，连续的时间窗口。
     * 例如：[0-10s), [10-20s), [20-30s)...
     * @param sourceStream 输入的 KStream
     */
    private void processTumblingWindow(KStream<String, String> sourceStream) {
        sourceStream
            // 5. groupByKey(): 将具有相同 Key 的记录分组。
            //    在这个例子中，因为我们没有显式指定分组的 Key，
            //    所以会使用消息本身的 Key 进行分组。
            //    这是进行聚合操作（如 count）前的必要步骤。
            .groupByKey()

            // 6. windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))):
            //    定义一个基于时间的窗口策略。
            //    - TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)):
            //      创建一个大小为 10 秒的滚动窗口。
            //      ofSizeWithNoGrace 表示窗口关闭后没有宽限期，数据不会延迟计入。
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))

            // 7. count():
            //    这是一个聚合操作 (有状态转换)。
            //    它会计算每个分组 (Key) 在每个窗口 (Window) 内的记录数量。
            //    返回类型是 KTable<Windowed<String>, Long>。
            //    Windowed<String> 是一个复合 Key，包含了原始 Key 和窗口信息。
            .count(Materialized.as(CLICK_COUNT_STORE))

            // 8. toStream():
            //    将 KTable 转换回 KStream，以便进行进一步的流式处理。
            //    转换后的 KStream 的 Key 是 Windowed<String>，Value 是计数值 Long。
            .toStream()

            // 9. map((windowedKey, value) -> ...):
            //    这是一个无状态转换。
            //    对流中的每一条记录进行转换。
            //    - windowedKey: 包含原始 Key 和窗口信息的 Windowed 对象。
            //    - value: 聚合后的计数值 (Long)。
            //    这里将复杂的 Windowed Key 和数值转换成一个更具可读性的字符串。
            .map((windowedKey, value) ->
                new KeyValue<>(windowedKey.key(), // 提取原始 Key
                    // 构造一个描述性字符串，包含窗口时间和计数值
                    "Window [" + windowedKey.window().startTime() + " - " +
                        windowedKey.window().endTime() + "]: Count = " + value))

            // 10. to(CLICK_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.String())):
            //     将处理后的 KStream 写入到一个新的 Kafka Topic。
            //     - CLICK_COUNT_OUTPUT: 目标 Topic 名称。
            //     - Produced.with(...): 指定 Key 和 Value 的序列化方式。
            //       因为我们已经将结果转换为 String，所以使用 String Serde。
            .to(CLICK_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
    }

    /**
     * 使用 Hopping Window（滑动窗口）计算用户在过去 5 分钟内的点击次数
     * Hopping Window: 固定窗口大小，可配置滑动步长，窗口可能重叠。
     * 例如：窗口大小 5 分钟，步长 1 分钟。
     * [0-5m), [1-6m), [2-7m)... 窗口之间有重叠。
     * @param sourceStream 输入的 KStream
     */
    private void processHoppingWindow(KStream<String, String> sourceStream) {
        sourceStream
            // 11. map((key, value) -> new KeyValue<>(extractUserId(value), value)):
            //     另一个无状态转换。
            //     重新组织数据的 Key。原始消息的 Key 可能不是用户 ID，
            //     所以我们从消息内容 Value 中提取用户 ID 作为新的 Key。
            //     这样后续的 groupByKey 就会按照用户 ID 来分组。
            .map((key, value) -> new KeyValue<>(extractUserId(value), value))

            // 12. groupByKey():
            //     现在根据新的 Key（即用户 ID）进行分组。
            .groupByKey()

            // 13. windowedBy(TimeWindows.ofSizeAndGrace(...).advanceBy(...)):
            //     定义滑动窗口策略。
            //     - ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)):
            //       窗口大小为 5 分钟，设置了 1 分钟的 grace period 用于处理延迟数据。
            //     - advanceBy(Duration.ofMinutes(1)):
            //       窗口每 1 分钟滑动一次（步长）。
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1))
                .advanceBy(Duration.ofMinutes(1)))

            // 14. count():
            //     聚合操作，计算每个用户 (Key) 在每个滑动窗口 (Window) 内的点击次数。
            .count(Materialized.as(USER_CLICK_RATE_STORE))

            // 15. toStream():
            //     将聚合结果 KTable 转回 KStream。
            .toStream()

            // 16. map((windowedKey, value) -> ...):
            //     无状态转换，格式化输出结果。
            .map((windowedKey, value) ->
                new KeyValue<>(windowedKey.key(), // 提取用户 ID 作为新 Key
                    // 构造描述性字符串，包含用户 ID、窗口时间和点击次数
                    "User " + windowedKey.key() +
                        " Window [" + windowedKey.window().startTime() + " - " +
                        windowedKey.window().endTime() + "]: Clicks = " + value))

            // 17. to(...):
            //     将结果写入到另一个 Kafka Topic。
            .to(USER_CLICK_RATE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
    }

    private String extractUserId(String event) {
        // 假设事件格式为 "userId:eventType:timestamp"
        return event.split(":")[0];
    }
}
