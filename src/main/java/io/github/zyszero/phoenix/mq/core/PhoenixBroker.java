package io.github.zyszero.phoenix.mq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topic
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:10
 */
public class PhoenixBroker {

    Map<String, PhoenixMq> mqMapping = new ConcurrentHashMap<>(64);

    public PhoenixMq find(String topic) {
        return mqMapping.get(topic);
    }

    public PhoenixMq createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new PhoenixMq(topic));
    }


    public PhoenixProducer createProducer() {
        return new PhoenixProducer(this);
    }

    public PhoenixConsumer<?> createConsumer(String topic) {
        PhoenixConsumer<?> consumer = new PhoenixConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }
}
