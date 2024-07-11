package io.unbong.ubmq.client;

import io.unbong.ubmq.module.UBMessage;

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
    String topic ;
    UBMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public UBConsumer(UBBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic)
    {
        this.topic = topic;
        mq = broker.find(topic);
        if(mq == null ) throw new RuntimeException("Topic not found");
    }

    public UBMessage<T> poll(long timeout)
    {
        return mq.poll(timeout);
    }

    public void addListener(UBListener<T> listener)
    {
        mq.addListener(listener);
    }
}
