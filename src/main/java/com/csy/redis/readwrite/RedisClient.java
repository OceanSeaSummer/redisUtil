package com.csy.redis.readwrite;

import redis.clients.jedis.Jedis;


public class RedisClient {
    private Jedis jedis;
    private RedisServer redisServer;

    public RedisClient(RedisServer redisServer) {
        this.jedis = redisServer.getJedisPool().getResource();
        this.redisServer = redisServer;
    }

    public Jedis getJedis() {
        return this.jedis;
    }

    public void returnResource(){
        try {
            if(jedis != null){
                redisServer.getJedisPool().returnResource(jedis);
            }
        } catch (Exception e) {
            throw new RedisException(e);
        }
    }

    public void returnBrokenResource() {
        try {
            if(jedis!= null){
                redisServer.getJedisPool().returnBrokenResource(jedis);
            }
        } catch (Exception e) {
            throw new RedisException(e);
        }
    }
}
