package io.unbong.ubmq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.unbong.ubmq.module.Result;
import io.unbong.ubmq.module.UBMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * broker for 
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:19
 */
@Slf4j
public class UBBroker {

    @Getter
    public static UBBroker Default = new UBBroker();

    public static String brokerURL ="http://127.0.0.1:8765/ubmq";
    static {
        init();
    }

    public static void init(){
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(()->{
            MultiValueMap<String, UBConsumer<?>> consums = getDefault().getConsumers();
            consums.forEach((topic,consumerList)->{
                consumerList.forEach(consumer->{
                    log.info("---> listener topic {} ", topic);
                    UBMessage<?> recv = consumer.recv(topic);
                    log.info("---> listener result {} ", recv);
                    if(recv == null) return;
                    try{
                        consumer.getListener().onMessage(recv);
                        consumer.ack(topic,recv);
                    }catch (Exception e)
                    {
                        //todo
                    }
                });
            });
        }, 100, 100);
    }
    @Getter
    private MultiValueMap<String, UBConsumer<?>> consumers = new LinkedMultiValueMap<>();
    public UBProducer createProducer(){
        return new UBProducer(this );
    }

    public UBConsumer<?> createConsumer(String topic){
        UBConsumer<?> consumer =  new UBConsumer(this );
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, UBMessage message) {
        log.info("---> send topic/message  {}/{}", topic, message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerURL + "/send?t=" + topic ,
                new TypeReference<Result<String>>(){});

        return result.getCode() == 1;

    }

    public <T> UBMessage<T> recv(String topic, String id) {
        log.info("---> recv topic/id  {}/{}", topic, id);

        Result<UBMessage<String>> result = HttpUtils.httpGet(
                brokerURL + "/recv?t=" + topic  +"&cid="+id,
                new TypeReference<Result<UBMessage<String>>>(){});
        log.info("---> recv result: {}",result);
        return (UBMessage<T>) result.getData();
    }

    public void sub(String topic, String id) {
        log.info("---> sub topic/id  {}/{}", topic, id);

        Result<String> result = HttpUtils.httpGet(
                brokerURL + "/sub?t=" + topic  +"&cid="+id,
                new TypeReference<Result<String>>(){});
        log.debug("---> sub result {}", result.toString());
    }

    public void unsub(String topic, String id) {
        log.info("---> unsub topic/id  {}/{}", topic, id);

        Result<String> result = HttpUtils.httpGet(
                brokerURL + "/unsub?t=" + topic  +"&cid="+id,
                new TypeReference<Result<String>>(){});
        log.debug("---> unsub result {}", result.toString());
    }

    public boolean ack(String topic, String cid, int offset){
        log.info("---> ack topic/cid/offset  {}/{}/{}}", topic, cid, offset);

        Result<String> result = HttpUtils.httpGet(
                brokerURL + "/ack?t=" + topic  +"&cid="+cid +"&offset="+offset,
                new TypeReference<Result<String>>(){});
        log.info("---> ack result {}", result.toString());
        return result.getCode() == 1;
    }

    public <T> void addConsumer(String topic, UBConsumer<T> consumer) {
        consumers.add(topic,consumer);
    }

}
