package io.github.zyszero.phoenix.mq.server;

import io.github.zyszero.phoenix.mq.model.PhoenixMessage;
import io.github.zyszero.phoenix.mq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * MQ Server
 *
 * @Author: zyszero
 * @Date: 2024/7/30 5:16
 */
@Controller
@RequestMapping("/phoenix-mq")
public class MQServer {


    // send
    @RequestMapping("/send")
    public Result<?> send(@RequestParam("topic") String topic,
                          @RequestParam("cid") String consumerId,
                          @RequestBody PhoenixMessage<String> message) {
        ;
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }

    // recv
    @RequestMapping("/recv")
    public Result<?> recv(@RequestParam("topic") String topic,
                          @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }


    // ack
    @RequestMapping("/ack")
    public Result<?> ack(@RequestParam("topic") String topic,
                         @RequestParam("cid") String consumerId,
                         @RequestParam("offset") Integer offset) {

        return Result.msg("" + MessageQueue.ack(topic, consumerId, offset));
    }


    // 1. sub
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("topic") String topic,
                                    @RequestParam("cid") String consumerId) {
        // 订阅
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }


    // unsub
    @RequestMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("topic") String topic,
                                      @RequestParam("cid") String consumerId) {
        // 取消订阅
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
