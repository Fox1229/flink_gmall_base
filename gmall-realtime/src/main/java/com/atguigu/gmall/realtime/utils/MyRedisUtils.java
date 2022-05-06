package com.atguigu.gmall.realtime.utils;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * Redis工具类
 */
public class MyRedisUtils {

    private static JedisPool jedisPool;

    public static void initJedisPoll() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000L);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT);
    }

    /**
     * 释放jedis连接
     */
    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * 获取jedis连接
     */
    public static Jedis getJedis() {

        if (jedisPool == null) {
            initJedisPoll();
        }

        return jedisPool.getResource();
    }
}
