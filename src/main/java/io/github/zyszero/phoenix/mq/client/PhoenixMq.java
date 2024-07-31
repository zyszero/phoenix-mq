package io.github.zyszero.phoenix.mq.client;

import io.github.zyszero.phoenix.mq.model.Message;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * mq for topics
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:10
 */
@AllArgsConstructor
public class PhoenixMq {

    public PhoenixMq(String topic) {
        this.topic = topic;
    }

    private String topic;
    private LinkedBlockingQueue<Message> queues = new LinkedBlockingQueue<>();

    private List<PhoenixListener> listeners = new ArrayList<>();

    public boolean send(Message message) {
        boolean offer = queues.offer(message);
        listeners.forEach(listener -> listener.onMessage(message));
        return offer;
    }


    /**
     * 拉模式获取消息
     *
     * @param timeout
     * @param <T>
     * @return
     */
    @SneakyThrows
    public <T> Message<T> poll(long timeout) {
        return queues.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(PhoenixListener<T> listener) {
        listeners.add(listener);
    }
}
