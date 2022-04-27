package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.app.func.MyDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * 业务数据分流
 * 实时数据：kafka
 * 纬度数据：hbase
 */
public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置分区
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点
        // 设置检查点插入时间，精准一次消费
        /*env.enableCheckpointing(5 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置两个检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 任务取消后是否删除检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend(""));
        // 操作hdfs的用户
        System.setProperty(HADOOP_USER_KEY, HADOOP_USER_NAME);
        // 检查点失败重试
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));*/

        // TODO 3.从kafka读取数据
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(ODS_BASE_DB, ODS_BASE_DB_GROUP_ID));

        // TODO 4.转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = kafkaDStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String logStr) throws Exception {
                        return JSON.parseObject(logStr);
                    }
                }
        );

        // TODO 5.对数据做简单的过滤
        SingleOutputStreamOperator<JSONObject> filterDStream = jsonObjDStream.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("table") != null
                                && jsonObject.getString("table").length() != 0
                                && jsonObject.getJSONObject("data") != null
                                && jsonObject.getString("data").length() > 3;
                    }
                }
        );

        // filterDStream.print();

        // TODO 6.读取MySQL配置表信息
        SourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname(MYSQL_HOST_NAME)
                .port(MYSQL_PORT)
                .databaseList(GMALL_CONFIG_DBS)
                .tableList(GMALL_CONFIG_TABLES)
                .username(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .deserializer(new MyDebeziumDeserializationSchema())
                // 获取发生变化的binlog
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlDStream = env.addSource(sourceFunction);
        // mysqlDStream.print();

        // TODO 7.将配置流数据进行广播：广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor", Types.STRING, Types.POJO(TableProcess.class));
        BroadcastStream<String> broadcastStream = mysqlDStream.broadcast(mapStateDescriptor);

        // TODO 8.将业务流数据与广播流进行关联：connect
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDStream.connect(broadcastStream);

        // TODO 9.将关联后的数据进行分流。事实数据：主流；纬度数据：测输出流
        // 定义侧输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbaseTag"){};
        SingleOutputStreamOperator<JSONObject> kafka
                = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        // 获取测输出流
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        // 打印
        kafka.print("kafka>>>");
        hbase.print("hbase>>>");

        // TODO 10.将测输出流的数据写入hbase
        hbase.addSink(new DimSinkFunction());

        // TODO 11.将主流中的数据写入kafka不同的topic
        kafka.addSink(
                MyKafkaUtils.getKafkaProducer(
                        new KafkaSerializationSchema<JSONObject>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                                return new ProducerRecord<byte[], byte[]>(
                                        element.getString(SINK_TABLE_KEY),
                                        element.getJSONObject("data").toJSONString().getBytes()
                                );
                            }
                        }
                )
        );

        env.execute();
    }
}
