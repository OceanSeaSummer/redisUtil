package com.csy.redis.readwrite;

import redis.clients.jedis.JedisPool;


public class RedisServer {

    private JedisPool jedisPool;

    private RedisServerType type;

    private boolean online = true;

    public RedisServer(JedisPool jedisPool, RedisServerType type) {
        this.jedisPool = jedisPool;
        this.type = type;
    }

    public RedisClient getClient() {
        return new RedisClient(this);
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public RedisServerType getType() {
        return type;
    }

    public void setType(RedisServerType type) {
        this.type = type;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }
}
