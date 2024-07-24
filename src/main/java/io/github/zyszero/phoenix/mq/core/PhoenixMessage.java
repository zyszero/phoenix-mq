package io.github.zyszero.phoenix.mq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * phoenix message model
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PhoenixMessage<T> {

    //    private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers; // 系统属性， AMQ-version = 1.0 X-version = 1.0
//    private Map<String, String> properties; // 业务属性

}
