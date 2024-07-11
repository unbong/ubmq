package io.unbong.ubmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Message Subscription
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-11 14:12
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;



}
