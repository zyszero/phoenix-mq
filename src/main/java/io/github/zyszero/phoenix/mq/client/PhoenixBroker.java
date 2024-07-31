package io.github.zyszero.phoenix.mq.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.zyszero.phoenix.mq.model.Message;
import io.github.zyszero.phoenix.mq.model.Result;
import io.github.zyszero.utils.HttpUtils;
import io.github.zyszero.utils.ThreadUtils;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * broker for topic
 *
 * @Author: zyszero
 * @Date: 2024/7/24 22:10
 */
@Data
public class PhoenixBroker {

    public static PhoenixBroker DEFAULT = new PhoenixBroker();

    public static String brokerUrl = "http://localhost:8765/phoenix-mq";


    static {
        init();
    }

    private static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, PhoenixConsumer<?>> consumerMultiValueMap = getDefault().getConsumers();
            consumerMultiValueMap.forEach((topic, consumers) -> {
                consumers.forEach(consumer -> {
                    Message<?> message = consumer.recv(topic);
                    if (message == null) return;
                    try {
                        consumer.getListener().onMessage(message);
                        consumer.ack(topic, message);
                    } catch (Exception e) {
                        // TODO 重试机制
                    }
                });
            });
        }, 100, 100);

    }


    private MultiValueMap<String, PhoenixConsumer<?>> consumers = new LinkedMultiValueMap<>();

    public static PhoenixBroker getDefault() {
        return DEFAULT;
    }

    public PhoenixProducer createProducer() {
        return new PhoenixProducer(this);
    }

    public PhoenixConsumer<?> createConsumer(String topic) {
        PhoenixConsumer<?> consumer = new PhoenixConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, Message<?> message) {
        System.out.println(" ==>> send topic/message: " + message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?topic=" + topic,
                new TypeReference<>() {
                });
        System.out.println(" ==>> send result: " + result);
        return result.isSuccess();
    }

    public void sub(String topic, String cid) {
        System.out.println(" ==>> sub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(
                brokerUrl + "/sub?topic=" + topic + "&cid=" + cid,
                new TypeReference<>() {
                });
        System.out.println(" ==>> sub result: " + result);
    }

    public void unsub(String topic, String cid) {
        System.out.println(" ==>> unsub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(
                brokerUrl + "/unsub?topic=" + topic + "&cid=" + cid,
                new TypeReference<>() {
                });
        System.out.println(" ==>> unsub result: " + result);
    }

    public <T> Message<T> recv(String topic, String cid) {
        System.out.println(" ==>> recv topic/cid：" + topic + "/" + cid);
        Result<Message<T>> result = HttpUtils.httpGet(
                brokerUrl + "/recv?topic=" + topic + "&cid=" + cid,
                new TypeReference<>() {
                });
        System.out.println(" ==>> recv result: " + result);
        return result.getData();
    }

    public boolean ack(String topic, String cid, int offset) {
        System.out.println(" ==>> ack topic/cid/offset: " + topic + "/" + cid + "/" + offset);
        Result<String> result = HttpUtils.httpGet(
                brokerUrl + "/ack?topic=" + topic + "&cid=" + cid + "&offset=" + offset,
                new TypeReference<>() {
                });
        System.out.println(" ==>> ack result: " + result);
        return result.isSuccess();
    }

    public <T> void addConsumer(String topic, PhoenixConsumer<T> consumer) {
        consumers.add(topic, consumer);
    }
}
