package io.github.zyszero.phoenix.mq.server;

import io.github.zyszero.phoenix.mq.model.Message;
import io.github.zyszero.phoenix.mq.store.Indexer;
import io.github.zyszero.phoenix.mq.store.Store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
//    private Message<?>[] queue = new Message[1024 * 10];
//    private int index = 0;

    private Store store;

    public MessageQueue(String topic) {
        this.topic = topic;
        store = new Store(topic);
    }


    public int send(Message<String> message) {
        int offset = store.position();
        message.getHeaders().put("X-offset", String.valueOf(offset));
        return store.write(message);
    }


    public Message<?> recv(int offset) {
        return store.read(offset);
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
        System.out.println(" ===>> sub: " + subscription);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub: " + subscription);
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }


    public static int send(String topic, Message<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        System.out.println(" ===>> send: topic/message = " + topic + "/" + message);
        return messageQueue.send(message);
    }

    public static Message<?> recv(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            Message<?> message = messageQueue.recv(offset);
            System.out.println(" ===>> recv: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
            System.out.println(" ===>> message : " + message);
            return message;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }



    public static Message<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                nextOffset = offset + entry.getLength();
            }
            Message<?> message = messageQueue.recv(nextOffset);
            System.out.println(" ===>> recv: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
            System.out.println(" ===>> message : " + message);
            return message;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


    /**
     * 批量消费指定主题的消息
     *
     * @param topic      主题名称，用于定位消息队列
     * @param consumerId 消费者标识，用于获取消费者的偏移量
     * @param size       消费的消息数量，限定返回的消息列表的最大长度
     * @return 包含指定数量消息的列表如果消费者或主题不存在，则抛出运行时异常
     */
    public static List<Message<?>> batch(String topic, String consumerId, int size) {
        // 根据主题名称获取对应的消息队列
        MessageQueue messageQueue = queues.get(topic);
        // 如果消息队列不存在，则抛出运行时异常
        if (messageQueue == null) throw new RuntimeException("topic not found");

        // 检查消费者是否已经订阅了该主题
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            // 获取当前消费者的偏移量
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            // 通过偏移量获取消息的索引信息
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            // 计算下一条消息的偏移量
            int nextOffset = offset + entry.getLength();
            // 初始化结果列表，用于存储消费的消息
            List<Message<?>> result = new ArrayList<>();

            // 接收并处理消息，直到达到指定数量或没有更多消息
            Message<?> message = messageQueue.recv(offset);
            while (message != null) {
                // 将消息添加到结果列表中
                result.add(message);
                // 如果达到指定的消息数量，停止处理
                if (result.size() >= size) {
                    break;
                }
                // 尝试接收下一条消息
                message = messageQueue.recv(nextOffset);
            }

            // 输出本次批量消费的消息的相关信息
            System.out.println(" ===>> batch: topic/cid/size = " + topic + "/" + consumerId + "/" + size);
            // 输出接收到的最后一条消息的信息
            System.out.println(" ===>> last message : " + message);
            // 返回消费的消息列表
            return result;
        }
        // 如果消费者未订阅该主题，则抛出运行时异常
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


    /**
     * 确认消费者对特定主题和偏移量的消息进行处理
     *
     * @param topic      主题名称，用于查找相应的消息队列
     * @param consumerId 消费者ID，用于识别特定的消费者
     * @param offset     消息的偏移量，表示消费者处理到的消息位置
     * @return 如果确认成功，返回处理的偏移量；如果失败，返回-1
     * @throws RuntimeException 如果找不到主题或消费者订阅，则抛出运行时异常
     */
    public static int ack(String topic, String consumerId, int offset) {
        // 根据主题获取消息队列
        MessageQueue messageQueue = queues.get(topic);
        // 如果消息队列为空，则抛出“主题未找到”异常
        if (messageQueue == null) throw new RuntimeException("topic not found");

        // 检查消息队列中是否存在指定消费者的订阅
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            // 获取消费者的订阅信息
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            // 检查偏移量是否在有效范围内（大于当前偏移量且小于最大长度）
            if (offset > subscription.getOffset() && offset < Store.LEN) {
                // 打印确认信息，包括主题、消费者ID和偏移量
                System.out.println(" ===>> ack: topic/cid/offset = "
                        + topic + "/" + consumerId + "/" + offset);
                // 更新消费者的偏移量
                subscription.setOffset(offset);
                // 返回处理的偏移量，表示确认成功
                return offset;
            }
            // 如果偏移量不在有效范围内，返回-1表示确认失败
            return -1;
        }
        // 如果找不到消费者的订阅，则抛出运行时异常
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


}
