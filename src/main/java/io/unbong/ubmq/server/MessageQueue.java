package io.unbong.ubmq.server;

import io.unbong.ubmq.module.UBMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * queue
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-11 13:43
 */
@Slf4j
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();

    private static final String TEST_TOPIC = "cn.kimking.test";
    static {

        queues.put(TEST_TOPIC,new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();


    private String topic;
    private UBMessage<?> [] queue= new UBMessage[1024*10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public static List<UBMessage<?>> batch(String topic, String consumerId, int size) {
        MessageQueue mq = queues.get(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        if(mq.subscriptions.containsKey(consumerId))   {
            int index = mq.subscriptions.get(consumerId).getOffset();
            int offset = index + 1;
            List<UBMessage<?>> result = new ArrayList<>();
            UBMessage<?> rec = mq.receive(offset);
            while(rec != null){
                result.add(rec);
                if(result.size()>= size) break;
                rec = mq.receive(++offset);
            }
            log.debug("---> batch: topic/cid/size/index, {}/{}/{}/{}",topic, consumerId,size, index);
            log.debug("---> batch last recv: message, {}", rec);
            return result;
        }
        throw new RuntimeException("subscription not found for topic/consumerId " + topic + " / " + consumerId) ;

    }

    public int send(UBMessage<?> message)
    {
        if (index >= queue.length){
            return -1;
        }
        message.getHeaders().put("X-offset", String.valueOf(index));
        queue[index++] = message;
        log.debug("---> send mq stored msg: {}", message);
        return  index;
    }



    public UBMessage<?> receive(int recIndex){
        if(recIndex <= index) return queue[recIndex];
        return null;
    }

    public void subscribe(MessageSubscription subscription){
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription){
        MessageQueue mq = queues.get(subscription.getTopic());
        if(mq == null) throw new RuntimeException("topic not found");
        log.debug("---> sub: subscription, {}",subscription);

        mq.subscribe(subscription);

    }

    public static void unsub(MessageSubscription subscription){
        MessageQueue mq = queues.get(subscription.getTopic());
        if(mq == null) throw null;
        log.debug("---> unsub: subscription, {}",subscription);

        mq.unsubscribe(subscription);

    }



    public static int send(String topic,  UBMessage<String> msg)
    {
        MessageQueue mq = queues.get(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        log.debug("---> send: topic/msg, {}/{}",topic, msg);

        return mq.send(msg);
    }

    public static UBMessage<?> recv(String topic, String consumerId, String cosumerId, int index)
    {
        MessageQueue mq = queues.get(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        if(mq.subscriptions.containsKey(consumerId))    return mq.receive(index);
        throw new RuntimeException("subscription not found for topic/consumerId " + topic + " / " + consumerId) ;
    }

    /**
     *  使用此方法，需要手动调用ack， 更新订阅关系里的offset
     *
     * @param topic
     * @param consumerId
     * @return
     */
    public static UBMessage<?> recv(String topic, String consumerId)
    {
        MessageQueue mq = queues.get(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        if(mq.subscriptions.containsKey(consumerId))   {
            int index = mq.subscriptions.get(consumerId).getOffset();
            log.debug("---> recv: topic/cid/index, {}/{}/{}",topic, consumerId, index+1);
            UBMessage<?> res = mq.receive(index+1);
            log.debug("---> recv: message, {}",res);
            return res;
        }
        throw new RuntimeException("subscription not found for topic/consumerId " + topic + " / " + consumerId) ;
    }


    public static int ack(String topic, String consumerId, int offset)
    {
        log.debug("---> ack: topic/cid/offset, {}/{}/{}",topic, consumerId, offset);
        MessageQueue mq = queues.get(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        if(mq.subscriptions.containsKey(consumerId))   {
            MessageSubscription messageSubscription = mq.subscriptions.get(consumerId);
            if(offset > messageSubscription.getOffset() && offset <= mq.index){

                messageSubscription.setOffset(offset);
                log.debug("---> ack result: topic/cid/offset, {}/{}/{}",topic, consumerId, offset);
                return offset;
            }
            return  -1;
        }
        throw new RuntimeException("subscription not found for topic/consumerId " + topic + " / " + consumerId) ;
    }
}


