package com.csy.redis.readwrite;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RedisShard {

    private int shardNo;

    private RedisServer master;

    private RedisServer slave;

    private static final int READ_TIME_OUT = 3 * 1000;

    private static final int MAX_WAIT = 15 * 1000;

    private static final int MAX_TOTAL = 25;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    //master|slave|pass|no
    //127.0.0.1:6379|127.0.0.1:6480|password|1
    private static final String CONFIG_FORMAT = "((\\d{1,3}\\.){3}\\d{1,3}:\\d{4,5}\\|)+.*\\|\\d";

    private Retry retry = new Retry();

    public RedisShard(String config) {
        if (config == null || !config.matches(CONFIG_FORMAT)) {
            throw new RedisException("config error");
        }
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(MAX_TOTAL);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setMaxWaitMillis(MAX_WAIT);
        String[] servers = config.split("|");
        String pass = servers[2];
        Integer no = Integer.parseInt(servers[3]);
        if (StringUtils.isBlank(pass)) {
            master = new RedisServer(new JedisPool(jedisPoolConfig, servers[0].split(":")[0], Integer.parseInt(servers[0].split(":")[1]), READ_TIME_OUT),
                    RedisServerType.MASTER);
            slave = new RedisServer(new JedisPool(jedisPoolConfig, servers[1].split(":")[0], Integer.parseInt(servers[1].split(":")[1]), READ_TIME_OUT),
                    RedisServerType.SLAVE);
        } else {
            master = new RedisServer(new JedisPool(jedisPoolConfig, servers[0].split(":")[0], Integer.parseInt(servers[0].split(":")[1]), READ_TIME_OUT, pass),
                    RedisServerType.MASTER);
            slave = new RedisServer(new JedisPool(jedisPoolConfig, servers[1].split(":")[0], Integer.parseInt(servers[1].split(":")[1]), READ_TIME_OUT, pass),
                    RedisServerType.SLAVE);
        }
        executor.execute(retry);
    }

    public int getShardNo() {
        return this.shardNo;
    }

    public RedisClient getRedisClient(boolean isReadOnly) {
        if (isReadOnly && slave.isOnline()) {
            return getForRead();
        } else if (master.isOnline()) {
            return getForReadWrite();
        } else {
            throw new RedisException("no server is online");
        }
    }

    private RedisClient getForRead() {
        try {
            return slave.getClient();
        } catch (Exception e) {
            onError(slave);
            return getForReadWrite();
        }
    }

    private RedisClient getForReadWrite() {
        try {
            return master.getClient();
        } catch (Exception e) {
            onError(master);
            throw new RedisException("shard: " + shardNo + " master down");
        }
    }

    private void onError(RedisServer server) {
        server.setOnline(false);
        retry.addRetry(server);
    }

    public void stop() {
        retry.exit = true;
        synchronized (retry) {
            retry.notifyAll();
        }
        executor.shutdown();
        master.getJedisPool().destroy();
        slave.getJedisPool().destroy();
    }

    final class Retry implements Runnable {
        volatile boolean exit = false;
        CopyOnWriteArraySet<RedisServer> set = new CopyOnWriteArraySet<RedisServer>();

        public void addRetry(RedisServer server) {
            set.add(server);
            synchronized (this) {
                notifyAll();
            }
        }

        public void run() {
            while (!exit) {
                for (RedisServer server : set) {
                    Jedis jedis = null;
                    try {
                        jedis = server.getJedisPool().getResource();
                        server.setOnline(true);
                        set.remove(server);
                    } catch (JedisConnectionException e) {
                        if (null != jedis) {
                            try {
                                server.getJedisPool().returnBrokenResource(jedis);
                            } catch (Exception e1) {
                                e1.printStackTrace();
                            }
                            jedis = null;
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                    } finally {
                        if (null != jedis) {
                            try {
                                server.getJedisPool().returnResource(jedis);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                synchronized (this) {
                    try {
                        wait(20 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
    }
}
