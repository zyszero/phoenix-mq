package io.github.zyszero.phoenix.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Result for MQServer
 *
 * @Author: zyszero
 * @Date: 2024/7/30 5:31
 */
@Data
@AllArgsConstructor
public class Result<T> {
    /**
     * 状态码
     * success: 1
     * fail: 0
     */
    private int code;

    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<String> ok(String message) {
        return new Result<>(1, message);
    }

    public static Result<PhoenixMessage<?>> msg(String message) {
        return new Result<>(1, PhoenixMessage.create(message, null));
    }

    public static Result<PhoenixMessage<?>> msg(PhoenixMessage<?> message) {
        return new Result<>(1, message);
    }
}
