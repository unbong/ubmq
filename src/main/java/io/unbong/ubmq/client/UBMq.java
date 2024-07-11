package io.unbong.ubmq.client;

import io.unbong.ubmq.module.UBMessage;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * mq for tppic
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:19
 */
@AllArgsConstructor
@NoArgsConstructor
public class UBMq {

    public UBMq(String topic) {
        this.topic = topic;
    }

    private String topic ;
    private LinkedBlockingQueue<UBMessage> queue = new LinkedBlockingQueue<>();
    private List<UBListener> listeners = new ArrayList<>();
    public boolean send(UBMessage message) {
        boolean  offered = queue.offer(message);
        listeners.forEach(listener-> listener.onMessage(message));
        return offered;
    }

    /**
     * 拉模式
     * @param timeout
     * @return
     * @param <T>
     */
    @SneakyThrows
    public <T> UBMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     *
     * @param listener
     * @param <T>
     */
    public <T> void addListener(UBListener<T> listener) {
        listeners.add(listener);
    }
}
