package io.unbong.ubmq.client;

import io.unbong.ubmq.module.UBMessage;

public interface UBListener<T> {

    void onMessage(UBMessage<T> message);
}
