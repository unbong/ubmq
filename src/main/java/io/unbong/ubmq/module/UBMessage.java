package io.unbong.ubmq.module;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * message model
 *
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 16:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UBMessage<T> {

    // private String topic;
    private Long id;
    private T boyd;
    private Map<String, String> headers; // 系统属性

    static AtomicLong idgen = new AtomicLong(0);

    public static long getId(){
        return idgen.getAndIncrement();
    }

    public static UBMessage<String> create(String body, Map<String,String> headers)
    {
        return new UBMessage<>(getId(), body, headers);
    }

    // private Map<String, String> properties; // 业务属性
}
