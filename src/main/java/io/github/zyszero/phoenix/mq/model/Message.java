package io.github.zyszero.phoenix.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * phoenix message model
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message<T> {

    //    private String topic;
    static AtomicLong idGen = new AtomicLong(0);

    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>(); // 系统属性， AMQ-version = 1.0 X-version = 1.0
//    private Map<String, String> properties; // 业务属性


    public static long nextId() {
        return idGen.getAndIncrement();
    }


    public static Message<String> create(String body, Map<String, String> headers) {
        return new Message<>(nextId(), body, headers);
    }
}
