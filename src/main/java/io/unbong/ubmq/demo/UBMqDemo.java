package io.unbong.ubmq.demo;

import com.alibaba.fastjson.JSON;
import io.unbong.ubmq.client.UBBroker;
import io.unbong.ubmq.client.UBConsumer;
import io.unbong.ubmq.module.UBMessage;
import io.unbong.ubmq.client.UBProducer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Description
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:41
 */
@Slf4j
public class UBMqDemo {

    public static void main(String[] args) throws IOException {
        long ids = 0;
        String topic = "cn.kimking.test";
        UBBroker broker = UBBroker.getDefault();

        UBProducer producer = broker.createProducer();
//        UBConsumer<?> consumer =broker.createConsumer(topic);
//        consumer.sub(topic);
//        consumer.addListener(message->{
//            System.out.println("onMessage-> " + message);
//        });

        UBConsumer<?> consumer1 = broker.createConsumer(topic);
//        consumer1.sub(topic);
        consumer1.addListener(topic, (message)->{
            System.out.println("---> onMessage " + message);
        });

        for (int i = 0; i < 10; i++) {
            Order order  = new Order(ids, "item", i*100);
            producer.send(topic, new UBMessage<>(ids++, JSON.toJSONString(order), null));
        }

//        for (int i = 0; i < 10; i++) {
//            UBMessage<String> message = (UBMessage<String>)consumer1.recv(topic);
//            System.out.println(message.toString());
//            consumer1.ack(topic, message);
//        }

        while(true){
            char c = (char)System.in.read();

            if(c == 'q'){
                consumer1.unsub(topic);
                break;
            }


            if(c == 'p')
            {
                Order order  = new Order(ids, "item", ids*100);
                producer.send(topic, new UBMessage<>((long)ids++, JSON.toJSONString(order), null));
            }

            if(c == 'c'){
                UBMessage<String> message = (UBMessage<String>)consumer1.recv(topic);
                System.out.println(message.toString());
                consumer1.ack(topic, message);
            }
        }
    }
}
