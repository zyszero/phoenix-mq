package io.github.zyszero.phoenix.mq.core;

import lombok.AllArgsConstructor;

/**
 * message producer
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:08
 */
@AllArgsConstructor
public class PhoenixProducer {

    PhoenixBroker broker;

    public boolean send(String topic, PhoenixMessage message) {

        PhoenixMq mq = broker.find(topic);

        if (mq == null) {
            throw new RuntimeException("topic not found");
        }

        return mq.send(message);
    }
}
