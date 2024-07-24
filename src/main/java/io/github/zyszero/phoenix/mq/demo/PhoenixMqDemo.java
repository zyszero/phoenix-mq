package io.github.zyszero.phoenix.mq.demo;

import io.github.zyszero.phoenix.mq.core.PhoenixBroker;
import io.github.zyszero.phoenix.mq.core.PhoenixConsumer;
import io.github.zyszero.phoenix.mq.core.PhoenixMessage;
import io.github.zyszero.phoenix.mq.core.PhoenixProducer;

import java.io.IOException;

/**
 * @Author: zyszero
 * @Date: 2024/7/24 22:26
 */
public class PhoenixMqDemo {

    public static void main(String[] args) throws IOException {

        long ids = 0;

        String topic = "phoenix.order";

        PhoenixBroker broker = new PhoenixBroker();
        broker.createTopic(topic);

        PhoenixProducer producer = broker.createProducer();
        PhoenixConsumer<?> consumer = broker.createConsumer(topic);
        consumer.listen(message -> System.out.println("onMessage => " + message));

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * i);
            producer.send(topic, new PhoenixMessage<>(ids++, order, null));
        }


        for (int i = 0; i < 10; i++) {
            PhoenixMessage<Order> message = (PhoenixMessage<Order>) consumer.poll(1000);
            System.out.println(message.getBody());
        }

        while (true) {
            char c = (char) System.in.read();

            if (c == 'q' || c == 'e') {
                break;
            }

            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new PhoenixMessage<>(ids++, order, null));
                System.out.println("send ok => " + order);
            }

            if (c == 'c') {
                PhoenixMessage<Order> message = (PhoenixMessage<Order>) consumer.poll(1000);
                System.out.println("poll ok => " + message);
            }

            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * i);
                    producer.send(topic, new PhoenixMessage<>(ids++, order, null));
                }
                System.out.println("send 10 orders...");
            }
        }
    }
}
