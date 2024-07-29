package io.github.zyszero.phoenix.mq.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Message Subscription
 *
 * @Author: zyszero
 * @Date: 2024/7/30 5:25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageSubscription {

    private String topic;

    private String consumerId;

    private int offset = -1;

}
