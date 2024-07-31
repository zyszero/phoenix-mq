package io.github.zyszero.phoenix.mq.client;

import io.github.zyszero.phoenix.mq.model.Message;
import lombok.AllArgsConstructor;

/**
 * message queue producer
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:08
 */
@AllArgsConstructor
public class PhoenixProducer {

    PhoenixBroker broker;

    public boolean send(String topic, Message<?> message) {
        return broker.send(topic, message);
    }
}
