package io.unbong.ubmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.unbong.ubmq.module.UBMessage;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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
 * Description
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-15 12:37
 */
@Slf4j
public class Store {

    public static final int SIZE = 1024 * 1024;
    private static final int MSG_LEN_BIAS = 10;
    String topic;

    public Store(String topic) {
        this.topic = topic;
    }

    @Getter
    MappedByteBuffer byteBuffer = null;
    @SneakyThrows
    public  void init(){
        File file = new File(topic+".dat");

        if(!file.exists()) file.createNewFile();
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel ch = (FileChannel) Files.newByteChannel(
                path, StandardOpenOption.READ, StandardOpenOption.WRITE
        );
        byteBuffer = ch.map(FileChannel.MapMode.READ_WRITE,0, SIZE);

        // todo 当服务重启后将写入位置指定为最后的位置
        // read files then point last position
        ByteBuffer readOnlyBuffer = byteBuffer.asReadOnlyBuffer();
        byte[] header = new byte[10];
        readOnlyBuffer.get(header);
        int nextPosition = 0;
        while(header[9]>0)
        {
            String trim = new String(header, StandardCharsets.UTF_8).trim();
            log.debug("---> msg header:{}", trim);
            int len = Integer.parseInt(trim);
            Indexer.addEntry(topic,nextPosition, len);
            nextPosition = nextPosition+len;
            readOnlyBuffer.position(nextPosition);
            log.debug("---> next position:{}", nextPosition);
            readOnlyBuffer.get(header);
        }
        readOnlyBuffer = null;
        log.debug("---> init position {}",nextPosition);
        byteBuffer.position(nextPosition);

    }

    public int write(UBMessage<String> message){

        int position = position();
        log.debug("---> write position {}", position);


        String msg = JSON.toJSONString(message);
        int len = msg.getBytes(StandardCharsets.UTF_8).length;
        len = len+ MSG_LEN_BIAS;
        Indexer.addEntry(topic,  position, len);
        msg = String.format("%10d%s",len,msg);
        log.debug("--->write content:{}",msg);
        byteBuffer.put(Charset.forName("UTF-8").encode(msg));
        return position;
    }

    public int position(){
        return byteBuffer.position();
    }

    public UBMessage<String> read(int offset){
        ByteBuffer readOnlyBuffer =  byteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        readOnlyBuffer.position(entry.getOffset() + MSG_LEN_BIAS);
        int len = entry.getLength()-MSG_LEN_BIAS;
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes, 0, len);
        String json = new String(bytes, Charset.forName("UTF-8"));

        log.debug("---> read from file,{}", json);
        // todo cut 10 byte then convert to UBMessage
        UBMessage<String> message = JSON.parseObject(json, new TypeReference<UBMessage<String>>(){});
        return message;
    }


}
