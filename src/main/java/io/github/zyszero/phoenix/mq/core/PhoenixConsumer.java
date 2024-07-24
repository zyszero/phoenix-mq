package io.github.zyszero.phoenix.mq.core;

/**
 * message consumer
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:19
 */
public class PhoenixConsumer<T> {

    PhoenixBroker broker;

    String topic;

    PhoenixMq mq;

    public PhoenixConsumer(PhoenixBroker broker) {
        this.broker = broker;
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
