package org.zewang.kafkademo.streams;


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author "Zewang"
 * @version 1.0
 * @description: 交互式查询功能
 * @email "Zewang0217@outlook.com"
 * @date 2025/11/03 18:53
 */

@Service
public class InteractiveQueryService {

    @Autowired
    private KafkaStreams kafkaStreams;

    public ReadOnlyWindowStore<String, Long> getWindowStore(String storeName) {
        return kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore())
        );
    }

    public boolean isKafkaStreamsRunning() {
        return kafkaStreams.state().isRunningOrRebalancing();
    }

    public String getKafkaStreamsState() {
        return kafkaStreams.state().toString();
    }
}
