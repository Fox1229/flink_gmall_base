package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * kafka工具类
 */
public class MyKafkaUtils {

    // kafka集群地址
    private static final String BOOTSTRAP_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    /**
     * 获取kafka消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
    }

    /**
     * 获取kafka生产者对象
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(BOOTSTRAP_SERVER, topic, new SimpleStringSchema());
    }
}
