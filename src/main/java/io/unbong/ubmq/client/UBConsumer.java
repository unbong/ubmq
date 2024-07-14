package io.unbong.ubmq.client;

import io.unbong.ubmq.module.UBMessage;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:35
 */
public class UBConsumer<T> {

    private String id; //
    UBBroker broker;
//    String topic ;

    static AtomicInteger idgen = new AtomicInteger(0);

    public UBConsumer(UBBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void sub(String topic)
    {
        broker.sub(topic, id);
    }

    public void unsub(String topic)
    {
        broker.unsub(topic, id);
    }

    public UBMessage<T> recv(String topic)
    {
        return broker.recv(topic, id);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public void addListener(String topic, UBListener<T> listener)
    {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    @Getter
    private UBListener listener;

    public boolean ack(String topic, UBMessage<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }
}
