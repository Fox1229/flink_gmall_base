package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

import static com.atguigu.gmall.realtime.common.GmallConfig.REDIS_KEY_EXPIRE;

/**
 * 查询纬度数据
 *
 * 获取维度数据    优化：旁路缓存
 * 思路：获取维度数据的时候，先从缓存中进行查询，如果缓存中存在维度数据，直接将缓存中的数据返回，缓存命中；如果缓存中不存在要查询的维度，
 *      再发送请求，到phoenix表中查询维度数据，并将查询的维度数据保存到缓存
 * 缓存产品的选择：
 *     状态：    堆内存、性能好、维护不方便
 *     Redis：  性能也不差，维护方便  √
 * 注意：
 *     缓存要设过期时间，不然冷数据会常驻缓存浪费资源
 *     要考虑维度数据是否会发生变化，如果发生变化要主动清除缓存
 * Redis中存放数据的类型：
 *     String
 * Redis中存放数据的key：
 *     dim:维度表表名:主键值1_主键值2
 */
public class MyDimUtils {

    /**
     * 根据id查询维度表数据
     * @param tableName 表名
     * @param id 主键id
     */
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    /**
     * 根据表名和主键查询数据
     * @param tableName 表名
     * @param colNameAndValues 主键名和主键值。可以有多个条件
     */
    @SafeVarargs
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValues) {

        // 查询数据时，先从redis缓存中查询，查询不到，再从hbase读取
        // 选型：String
        // 保存：set
        // 读取：get
        // key：dim_tableName_columnValue1_columnValue2...
        // Value：obj
        // 过期：60 * 60 * 24 * 7
        StringBuilder redisKey = new StringBuilder();
        // 拼接redis保存的key
        redisKey.append("dim:").append(tableName.toLowerCase()).append(":");

        // select * from dim_base_trademark where id = '15' and name = 'tom';
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" where ");

        for (int i = 0; i < colNameAndValues.length; i++) {

            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String columnName = colNameAndValue.f0;
            String columnValue = colNameAndValue.f1;

            redisKey.append(columnValue);

            sb.append(columnName).append(" = '").append(columnValue).append("'");

            if (i != colNameAndValues.length - 1) {
                redisKey.append("_");
                sb.append(" and ");
            }
        }

        String jsonInfoStr = null;
        JSONObject jsonObj = null;

        // 读取redis数据
        Jedis jedis = MyRedisUtils.getJedis();
        jsonInfoStr = jedis.get(redisKey.toString());

        if (jsonInfoStr != null && jsonInfoStr.length() > 0) {
            // 从redis中查询到数据
            jsonObj = JSON.parseObject(jsonInfoStr);
        } else {

            // 从redis中查询不到数据，到Phoenix表中查询纬度数据
            System.err.println("从Redis查询数据失败...");
            // 没有从redis查询到数据，继续从hbase中查询数据
            System.out.println("hbase sql: " + sb);

            List<JSONObject> resList = MyPhoenixUtils.queryList(sb.toString(), JSONObject.class);

            if (resList.size() > 0) {
                jsonObj = resList.get(0);

                // 将查询到的结果保存到redis
                jedis.setex(redisKey.toString(), REDIS_KEY_EXPIRE, jsonObj.toJSONString());
            } else {
                System.err.println("未查询到指定的数据！");
            }
        }

        // 释放Redis连接
        MyRedisUtils.close(jedis);

        return jsonObj;
    }

    /**
     * 删除Redis缓存hbase纬度信息
     */
    public static void deleteRedisCache(String tableName, String... sinkPk) {

        StringBuilder redisKey = new StringBuilder();
        redisKey.append("dim:").append(tableName.toLowerCase()).append(":");
        for (int i = 0; i < sinkPk.length; i++) {
            redisKey.append(sinkPk[i]);

            if (i != sinkPk.length - 1) {
                redisKey.append("_");
            }
        }

        // 获取redis连接
        Jedis jedis = null;
        try {
            jedis = MyRedisUtils.getJedis();
            jedis.del(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Redis缓存数据删除失败！");
        } finally {
            MyRedisUtils.close(jedis);
        }
    }
}
