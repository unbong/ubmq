package io.unbong.ubmq.server;

import io.unbong.ubmq.module.Result;
import io.unbong.ubmq.module.UBMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * MQ server
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-11 13:28
 */
@Controller()
@RequestMapping("/ubmq")
public class MQServer {



    // send
    @RequestMapping("/send")
    public Result send(@RequestParam("t") String topic, @RequestParam("cid") String consumerId,
                       @RequestBody UBMessage<String> message){


        return Result.ok(""+MessageQueue.send(topic, consumerId, message));
    }

    //recv
    @RequestMapping("/recv")
    public Result<UBMessage<?>> receive(@RequestParam("t") String topic, @RequestParam("cid") String consumerId     ){
        return Result.msg("hello"+MessageQueue.recv(topic,consumerId));
    }

    //ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset
                                ){


        return Result.ok(""+MessageQueue.ack(topic, consumerId, offset));
    }

    // subscribe
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId){

        MessageQueue.sub(new MessageSubscription(topic,consumerId, -1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId){

        MessageQueue.unsub(new MessageSubscription(topic,consumerId,-1));
        return Result.ok();
    }


}
