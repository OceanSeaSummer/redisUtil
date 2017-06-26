package com.csy.redis.readwrite;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.MurmurHash;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class RedisHelper {

    private static final int HASH_SEED = 3453;

    private static final String[] readOnlyCommands = new String[]{"get", "type", "ttl", "getbit", "getrange", "substr", "hget",
            "hmget", "hexists", "hlen", "hkeys", "hvals", "hgetAll", "llen", "lrange", "lindex", "smembers", "scard",
            "sismember", "srandmember", "zrange", "zrank", "zrevrank", "zrevrange", "zcard", "zscore", "zcount"};
    private static Map<Integer, RedisShard> shards;
    private static String serverConfig;
    private static boolean isRedisEnable = false;
    private static boolean isStarted = false;

    static{
        init();
    }

    private static void init() {
        loadProperties();
        if (isRedisEnable) {
            shards = new HashMap<Integer, RedisShard>();
            String[] servers = serverConfig.split(";");
            for (int i = 0; i < servers.length; i++) {
                shards.put(i, new RedisShard(servers[i]));
            }
            isStarted = true;
        }
    }

    private static void loadProperties() {
        Properties p = new Properties();
        try {
            p.load(RedisHelper.class.getResourceAsStream("/application.properties"));

        } catch (IOException e) {
            throw new RedisException("load placeholder.properties failed.");
        }
        if (StringUtils.isBlank(p.getProperty("redis.enable"))) {
            throw new RedisException("redis.enable not found in placeholder.properties.");
        } else {
            if (StringUtils.equalsIgnoreCase(p.getProperty("redis.enable"), "true")) {
                if (StringUtils.isBlank(p.getProperty("redis.servers"))) {
                    throw new RedisException("redis.servers not found in placeholder.properties.");
                }
                    serverConfig = p.getProperty("redis.servers");
                isRedisEnable = true;
            }
        }

    }

    private RedisHelper() {

    }

    public static JedisCommands getJedis(){
        if(isRedisEnable && isStarted) {
            return (JedisCommands) Proxy.newProxyInstance(RedisHelper.class.getClassLoader(),
                    new Class[]{JedisCommands.class},new ShardedInvocationHandler());

        } else {
            throw new RedisException("redis is not started.");
        }
    }

    public static BinaryJedisCommands getBinaryJedis() {
        if (isRedisEnable && isStarted) {
            return (BinaryJedisCommands) Proxy.newProxyInstance(BinaryJedisCommands.class.getClassLoader(),
                    new Class[]{BinaryJedisCommands.class}, new ShardedInvocationHandler());
        } else {
            throw new RedisException("redis is not started.");
        }
    }

    private static class ShardedInvocationHandler implements InvocationHandler{

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String command = method.getName();
            int shardNo = 0;
            if(args != null && args.length > 0) {
                if(args[0] instanceof String) {
                    shardNo = Math.abs(MurmurHash.hash(((String)args[0]).getBytes(), HASH_SEED) % shards.size());
                } else if(args[0] instanceof byte[]) {
                    shardNo = Math.abs(MurmurHash.hash((byte[]) args[0], HASH_SEED) % shards.size());
                }
            }
            RedisClient client = null;
            Object result = null;
            boolean isBroken = false;
            try {
                client = shards.get(shardNo).getRedisClient(isReadOnly(command));
                method.invoke(client,args);
            } catch (JedisConnectionException e) {
                isBroken = true;
                if(client != null){
                    client.returnBrokenResource();
                }
                throw e;
            } finally {
                if(!isBroken && client != null) {
                    client.returnResource();
                }
            }
            return result;
        }
    }



    private static boolean isReadOnly(String command) {
        for (String s : readOnlyCommands) {
            if (StringUtils.equalsIgnoreCase(s, command)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRedisEnable() {
        return isRedisEnable;
    }

    public static void stop() {
        if (isRedisEnable && isStarted) {
            isStarted = false;
            for (RedisShard rs : shards.values()) {
                rs.stop();
            }
        }
    }

    public static void start() {
        if (isRedisEnable && !isStarted) {
            init();
        }
    }
}
