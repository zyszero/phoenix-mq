package io.github.zyszero.phoenix.mq.client;

import io.github.zyszero.phoenix.mq.model.PhoenixMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:19
 */
public class PhoenixConsumer<T> {

    private String id;

    PhoenixBroker broker;

    String topic;

    PhoenixMq mq;

    static AtomicInteger idGen = new AtomicInteger(0);

    public PhoenixConsumer(PhoenixBroker broker) {
        this.broker = broker;
        this.id = "CID" + idGen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        PhoenixMq mq = broker.find(topic);
        if (mq == null) {
            throw new RuntimeException("topic not found");
        }
        this.mq = mq;
    }

    public PhoenixMessage<T> poll(long timeout) {
        return mq.poll(timeout);
    }


    public void listen(PhoenixListener<T> listener) {
        mq.addListener(listener);
    }
}
