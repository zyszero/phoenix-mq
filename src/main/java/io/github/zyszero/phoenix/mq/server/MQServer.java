package io.github.zyszero.phoenix.mq.server;

import io.github.zyszero.phoenix.mq.model.Message;
import io.github.zyszero.phoenix.mq.model.Result;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * MQ Server
 *
 * @Author: zyszero
 * @Date: 2024/7/30 5:16
 */
@RestController
@RequestMapping("/phoenix-mq")
public class MQServer {


    // send
    @PostMapping("/send")
    public Result<?> send(@RequestParam("topic") String topic,
                          @RequestBody Message<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    // recv
    @GetMapping("/recv")
    public Result<Message<?>> recv(@RequestParam("topic") String topic,
                                   @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    @GetMapping("/batch")
    public Result<List<Message<?>>> batch(@RequestParam("topic") String topic,
                                          @RequestParam("cid") String consumerId,
                                          @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
    }


    // ack
    @GetMapping("/ack")
    public Result<?> ack(@RequestParam("topic") String topic,
                         @RequestParam("cid") String consumerId,
                         @RequestParam("offset") Integer offset) {

        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }


    // 1. sub
    @GetMapping("/sub")
    public Result<String> subscribe(@RequestParam("topic") String topic,
                                    @RequestParam("cid") String consumerId) {
        // 订阅
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }


    // unsub
    @GetMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("topic") String topic,
                                      @RequestParam("cid") String consumerId) {
        // 取消订阅
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
