package com.csy.redis.readwrite;


public class RedisException extends RuntimeException {

    public RedisException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisException(Throwable cause) {

        super(cause);
    }

    public RedisException(String message) {
        super(message);
    }
}
