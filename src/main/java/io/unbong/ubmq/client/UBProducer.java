package io.unbong.ubmq.client;

import io.unbong.ubmq.module.UBMessage;

/**
 * Description
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:17
 */
public class UBProducer {

    UBBroker broker;

    public UBProducer(UBBroker broker) {
        this.broker = broker;
    }

    public boolean send(String topic, UBMessage message)
    {

        UBMq mq = broker.find(topic);
        if(mq == null)
            throw new RuntimeException("topic not found");
        return  mq.send(message);
    }
}
