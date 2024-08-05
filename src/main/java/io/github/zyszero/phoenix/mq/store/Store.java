package io.github.zyszero.phoenix.mq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.zyszero.phoenix.mq.model.Message;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Message store.
 *
 * @Author: zyszero
 * @Date: 2024/8/1 21:50
 */
public class Store {

    public static final int LEN = 1024 * 1000;

    private String topic;

    @Getter
    MappedByteBuffer mappedByteBuffer = null;


    public Store(String topic) {
        this.topic = topic;
        init();
    }

    @SneakyThrows
    public void init() {
        File file = new File(topic + ".dat");
        if (!file.exists()) file.createNewFile();

        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);

        // todo 存在的问题：每次启动服务，会覆盖之前的数据，因为 init_position 初始值是 0

        // todo 1、读取索引
        // 判断是否有数据
        // 读前10位，转成int=len，看是不是大于0，往后翻len的长度，就是下一条记录，
        // 重复上一步，一直到0为止，找到数据结尾
        // mappedByteBuffer.position(init_pos);
        // todo 2、如果总数据 > 10M，使用多个数据文件的list来管理持久化数据
        // 需要创建第二个数据文件，怎么来管理多个数据文件。
    }


    public int position() {
        return mappedByteBuffer.position();
    }

    public int write(Message<String> message) {
        System.out.println(" write pos -> " + mappedByteBuffer.position());
        String msg = JSON.toJSONString(message);
        int position = mappedByteBuffer.position();
        Indexer.addEntry(topic, position, msg.getBytes(StandardCharsets.UTF_8).length);

        mappedByteBuffer.put(StandardCharsets.UTF_8.encode(msg));
        return position;
    }

    public Message<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        readOnlyBuffer.position(entry.getOffset());

        int len = entry.getLength();
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes, 0, len);

        String json = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(" read json ==>> " + json);
        return JSON.parseObject(json, new TypeReference<>() {
        });
    }
}
