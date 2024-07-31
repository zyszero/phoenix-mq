package io.github.zyszero.phoenix.mq.client;


import io.github.zyszero.phoenix.mq.model.Message;

/**
 * message listener
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:38
 */
public interface PhoenixListener<T> {

    void onMessage(Message<T> message);
}
