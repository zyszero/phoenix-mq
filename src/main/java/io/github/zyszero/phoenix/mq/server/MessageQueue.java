package io.github.zyszero.phoenix.mq.server;

import io.github.zyszero.phoenix.mq.model.PhoenixMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * queues.
 *
 * @Author: zyszero
 * @Date: 2024/7/30 5:19
 */
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();

    private static final String TEST_TOPIC = "io.github.zyszero.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
    private PhoenixMessage<?>[] queue = new PhoenixMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }


    public int send(PhoenixMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }


    public PhoenixMessage<?> recv(int idx) {
        if (idx <= index) {
            return queue[idx];
        }
        return null;
    }


    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }


    public static int send(String topic, String consumerId, PhoenixMessage<?> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static PhoenixMessage<?> recv(String topic, String consumerId, int idx) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(idx);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


    /**
     * 从指定的主题和消费者ID中接收消息。
     * 注意⚠️：使用此方法，需要手工调用 ack，更新订阅关系里的 offset。
     *
     * @param topic      消息的主题，用于定位消息队列。
     * @param consumerId 消费者的唯一标识，用于定位消费者的订阅信息。
     * @return 返回特定消费者ID下的下一条消息。
     * @throws RuntimeException 如果主题不存在或消费者ID未订阅该主题，则抛出运行时异常。
     */
    public static PhoenixMessage<?> recv(String topic, String consumerId) {
        // 通过主题获取消息队列
        MessageQueue messageQueue = queues.get(topic);
        // 如果主题不存在，则抛出运行时异常
        if (messageQueue == null) throw new RuntimeException("topic not found");
        // 检查消费者ID是否订阅了该主题
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            // 获取消费者ID的偏移量
            int idx = messageQueue.subscriptions.get(consumerId).getOffset();
            // 根据偏移量从消息队列中接收消息
            return messageQueue.recv(idx);
        }
        // 如果消费者ID未订阅该主题，则抛出运行时异常
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }



    /**
     * 确认消息消费的偏移量。
     *
     * 此方法用于消费者在消费消息后，确认其消费到的最新消息的偏移量。这有助于跟踪消费者的消费进度，并在需要时重置或恢复消费。
     *
     * @param topic 消息的主题，用于定位消息队列。
     * @param consumerId 消费者的唯一标识，用于确定消费者在特定主题下的订阅关系。
     * @param offset 消费者希望确认的最新消息的偏移量。
     * @return 如果确认成功，则返回最新的偏移量；如果确认失败，则返回-1。
     * @throws RuntimeException 如果主题不存在或消费者对该主题没有订阅，则抛出运行时异常。
     */
    public static int ack(String topic, String consumerId, int offset) {
        // 根据主题获取消息队列，如果主题不存在，则抛出运行时异常。
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");

        // 检查消费者是否订阅了该主题，如果没有订阅，则抛出运行时异常。
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            // 如果确认的偏移量大于当前订阅的偏移量且小于消息队列的索引，则更新订阅的偏移量并返回确认的偏移量。
            if (offset > subscription.getOffset() && offset < messageQueue.index) {
                subscription.setOffset(offset);
                return offset;
            }
            // 如果确认的偏移量不满足条件，则返回-1表示确认失败。
            return -1;
        }
        // 如果主题存在但消费者没有订阅，则抛出运行时异常。
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


}
