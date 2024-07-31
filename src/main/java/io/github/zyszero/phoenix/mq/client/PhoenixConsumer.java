package io.github.zyszero.phoenix.mq.client;

import io.github.zyszero.phoenix.mq.model.Message;
import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:19
 */
@Data
public class PhoenixConsumer<T> {

    private String id;

    private PhoenixListener listener;

    PhoenixBroker broker;


    static AtomicInteger idGen = new AtomicInteger(0);

    public PhoenixConsumer(PhoenixBroker broker) {
        this.broker = broker;
        this.id = "CID" + idGen.getAndIncrement();
    }

    public void sub(String topic) {
        this.broker.sub(topic, id);
    }

    public void unsub(String topic) {
        this.broker.unsub(topic, id);
    }

    public Message<T> recv(String topic) {
        return broker.recv(topic, id);
    }


    public void ack(String topic, Message<?> message) {
        if (message == null) {
            return;
        }
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        this.broker.ack(topic, id, offset);
    }

    public void listen(String topic, PhoenixListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }
}
