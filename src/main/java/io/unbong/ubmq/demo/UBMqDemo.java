package io.unbong.ubmq.demo;

import io.unbong.ubmq.client.UBBroker;
import io.unbong.ubmq.client.UBConsumer;
import io.unbong.ubmq.module.UBMessage;
import io.unbong.ubmq.client.UBProducer;

import java.io.IOException;

/**
 * Description
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:41
 */
public class UBMqDemo {

    public static void main(String[] args) throws IOException {
        long ids = 0;

        String topic = "ub.order";
        UBBroker broker = new UBBroker();
        broker.createTopic(topic);

        UBProducer producer = broker.createProducer();
        UBConsumer<?> consumer =broker.createConsumer(topic);
        consumer.subscribe(topic);
        consumer.addListener(message->{
            System.out.println("onMessage-> " + message);
        });

        for (int i = 0; i < 10; i++) {
            Order order  = new Order(ids, "item", i*100);
            producer.send(topic, new UBMessage<>((long)ids++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            UBMessage<?> message = consumer.poll(1000);
            System.out.println(message.toString());
        }

        while(true){
            char c = (char)System.in.read();

            if(c == 'q')
                break;

            if(c == 'p')
            {
                Order order  = new Order(ids, "item", ids*100);
                producer.send(topic, new UBMessage<>((long)ids++, order, null));
            }

            if(c == 'c'){
                UBMessage<?> message = consumer.poll(1000);
                System.out.println(message.toString());
            }
        }
    }
}
