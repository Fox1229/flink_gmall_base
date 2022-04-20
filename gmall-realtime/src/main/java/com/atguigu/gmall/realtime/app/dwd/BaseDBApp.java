package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.atguigu.gmall.realtime.app.func.MyDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * 业务数据分流
 */
public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);

        // TODO 2.设置检查点
        // 开启检查点
        /*env.enableCheckpointing(5 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置job取消，是否删除检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点最小时间间隔
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2000);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 设置状态后端、操作用户
        env.setStateBackend(new FsStateBackend(""));
        System.setProperty("HADOOP_USERNAME", "atguigu");*/

        // TODO 3.从kafka消费数据
        String topic = "f_ods_base_db_m";
        String groupId = "f_ods_base_db_m_gid";
        // 获取消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        // 消费数据：封装流
        DataStreamSource<String> kafkaDStream = env.addSource(kafkaSource);

        // TODO 4.转换结构
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = kafkaDStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String logStr) throws Exception {
                        return JSON.parseObject(logStr);
                    }
                }
        );

        // TODO 5.对业务数据进行ETL
        SingleOutputStreamOperator<JSONObject> etlDStream = jsonObjDStream.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        boolean flg = jsonObject.getString("table") != null
                                && jsonObject.getString("table").length() != 0
                                && jsonObject.getJSONObject("data") != null
                                && jsonObject.getString("data").length() > 3;
                        return flg;
                    }
                }
        );

        // etlDStream.print();

        // TODO 6.使用FinkCDC从MySQL读取配置表数据
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("flink_gmall_config")
                .tableList("flink_gmall_config.table_process")
                .username("root")
                .password("123456")
                .deserializer(new MyDebeziumDeserializationSchema())
                // 获取发生变化的binlog
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> flinkCdcDStream = env.addSource(sourceFunction);
        flinkCdcDStream.print();


        // TODO 7.



        env.execute();
    }
}
