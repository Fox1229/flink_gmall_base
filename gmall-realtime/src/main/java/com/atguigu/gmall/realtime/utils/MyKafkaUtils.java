package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.util.Properties;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * kafka工具类
 */
public class MyKafkaUtils {

    /**
     * 获取kafka消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
    }

    /**
     * 获取kafka生产者对象。写入指定topic
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        // 不能保证精准一次消费
        // return new FlinkKafkaProducer<String>(KAFKA_SERVERS, topic, new SimpleStringSchema());

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        // 事务的第二阶段提交需要在检查点提交之后进行，即15min(默认事务最大提交时间) > TransactionTimeout > CheckPointTimeout
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, PRODUCER_TRANSACTION_TIMEOUT);
        return new FlinkKafkaProducer<String>(
                KAFKA_DEFAULT_TOPIC,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(
                                topic,
                                element.getBytes()
                        );
                    }
                },
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    /**
     * 获取kafka生产者对象。动态写入topic
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        // 事务的第二阶段提交需要在检查点提交之后进行，即 15min(默认事务最大提交时间) > TransactionTimeout > CheckPointTimeout
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, PRODUCER_TRANSACTION_TIMEOUT);
        return new FlinkKafkaProducer<T>(
                KAFKA_DEFAULT_TOPIC,
                kafkaSerializationSchema,
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    /**
     * 获取FlinkSQL连接kafka的相关属性配置
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return "'connector' = 'kafka'," +
                "'topic' = '" + topic + "'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'properties.group.id' = '" + groupId + "'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'";
    }
}
