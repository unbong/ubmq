package io.unbong.ubmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for 
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:19
 */
public class UBBroker {

    Map<String, UBMq> mqMapping = new ConcurrentHashMap<>(64);

    public UBMq find(String topic) {

        return mqMapping.get(topic);
    }

    public UBMq createTopic(String topic){
        return mqMapping.putIfAbsent(topic, new UBMq(topic));

    }

    public UBProducer createProducer(){
        return new UBProducer(this );
    }

    public UBConsumer<?> createConsumer(String topic){
        UBConsumer<?> consumer =  new UBConsumer(this );
        consumer.subscribe(topic);
        return consumer;
    }
}
