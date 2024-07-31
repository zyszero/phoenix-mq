package io.github.zyszero.phoenix.mq.demo;

import com.alibaba.fastjson.JSON;
import io.github.zyszero.phoenix.mq.client.PhoenixBroker;
import io.github.zyszero.phoenix.mq.client.PhoenixConsumer;
import io.github.zyszero.phoenix.mq.model.Message;
import io.github.zyszero.phoenix.mq.client.PhoenixProducer;

import java.io.IOException;

/**
 * @Author: zyszero
 * @Date: 2024/7/24 22:26
 */
public class PhoenixMqDemo {

    public static void main(String[] args) throws IOException {

        long ids = 0;

        String topic = "io.github.zyszero.test";

        PhoenixBroker broker = PhoenixBroker.getDefault();
        PhoenixProducer producer = broker.createProducer();
//        PhoenixConsumer<?> consumer = broker.createConsumer(topic);
//        consumer.sub(topic);

        PhoenixConsumer<?> consumer = broker.createConsumer(topic);
        consumer.listen(topic, message -> {
            System.out.println("onMessage => " + message); // 这里模拟处理消息
        });
//        consumer.sub(topic);

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * i);
            Message<String> message = new Message<>(ids++, JSON.toJSONString(order), null);
            System.out.println(" ===>> send message: " + JSON.toJSONString(message));
            producer.send(topic, message);
        }


        for (int i = 0; i < 10; i++) {
            Message<Order> message = (Message<Order>) consumer.recv(topic);
            System.out.println(message.getBody()); // 模拟业务处理
            consumer.ack(topic, message);
        }

        while (true) {
            char c = (char) System.in.read();

            if (c == 'q' || c == 'e') {
                consumer.unsub(topic);
                break;
            }

            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
                System.out.println("produce ok => " + order);
            }

            if (c == 'c') {
                Message<Order> message = (Message<Order>) consumer.recv(topic);
                System.out.println("consumer ok => " + message);
                consumer.ack(topic, message);
            }

            if (c == 'b') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * i);
                    producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null));
                }
                System.out.println("produce 10 orders...");
            }
        }
    }
}
