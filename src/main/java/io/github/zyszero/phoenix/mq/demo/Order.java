package io.github.zyszero.phoenix.mq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: zyszero
 * @Date: 2024/7/24 22:25
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private long id;

    private String data;

    private double price;
}
