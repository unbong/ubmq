package io.unbong.ubmq.module;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * result for mqServer
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-11 14:27
 */
@Data
@AllArgsConstructor
public class Result<T> {
    private int code; // 1: success  , 0: fail
    private T data;

    public static Result<String> ok(){
        return new Result(1, "OK");
    }

    public static Result<UBMessage<?>> msg(String msg) {
        return new Result<>(1, UBMessage.create(msg, null));
    }

    public static Result<UBMessage<?>> msg(UBMessage msg) {
        return new Result<>(1, msg);
    }

    public static Result ok(String msg) {
        return new Result(1, msg);
    }

    public static Result<List<UBMessage<?>>> msg(List<UBMessage<?>> msg) {
        return  new Result<>(1, msg );
    }
}
