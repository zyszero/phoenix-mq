package io.github.zyszero.phoenix.mq.core;

/**
 * message listener
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:38
 */
public interface PhoenixListener<T> {

    void onMessage(PhoenixMessage<T> message);
}
