package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLogApp1 {

    public static void main(String[] args) throws Exception {

        // TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2.设置检查点
        // 开启检查点
        /*env.enableCheckpointing(5 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点保存路径
        env.setStateBackend(new FsStateBackend(""));
        // 设置用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 设置两个检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 设置取消job后，是否删除检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置失败重试次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));*/

        // TODO 3.获取kafka对象，读取指定topic的数据
        String topic = "f_ods_base_log";
        String groupId = "f_ods_base_log_gid";
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtils.getKafkaSource(topic, groupId));

        //TODO 4.转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = kafkaDStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String logStr) throws Exception {
                        return JSON.parseObject(logStr);
                    }
                }
        );

        // TODO 5.修复新老访客字段: 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjIsNewDStream = jsonObjDStream
                // 根据mid进行分组
                .keyBy(r -> r.getJSONObject("common").getString("mid"))
                .map(
                        new RichMapFunction<JSONObject, JSONObject>() {

                            // 维护用户上次登录时间
                            private ValueState<String> lastVisitDateState;
                            // 格式化时间
                            private SimpleDateFormat sdf;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                lastVisitDateState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<String>("lastVisitDateState", Types.STRING)
                                );

                                sdf = new SimpleDateFormat("yyyyMMdd");
                            }

                            @Override
                            public JSONObject map(JSONObject jsonObject) throws Exception {

                                // 获取is_new字段信息
                                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                                // 对“新”用户进行处理
                                if ("1".equals(isNew)) {
                                    // 获取mid历史登录状态
                                    String lastVisitDate = lastVisitDateState.value();
                                    // 当前时间
                                    String currentVisitDate = sdf.format(jsonObject.getLong("ts"));

                                    // 判断当前状态中是否包含登录的mid，若存在，说明非首次登录，修改is_new字段为0
                                    if (lastVisitDate != null && lastVisitDate.length() != 0) {
                                        // 同一天不做更新
                                        if (!lastVisitDate.equals(currentVisitDate)) {
                                            isNew = "0";
                                            jsonObject.getJSONObject("common").put("is_new", isNew);
                                        }
                                    } else {
                                        // 不存在登录状态即为首次登录，更新状态
                                        lastVisitDateState.update(currentVisitDate);
                                    }
                                }

                                return jsonObject;
                            }
                        }
                );

        // TODO 6.分流：测输出流

        // 定义测输出流
        // 启动
        OutputTag<String> startOutputDSteam = new OutputTag<String>("startOutputDSteam"){};
        // 曝光
        OutputTag<String> displayOutputDSteam = new OutputTag<String>("displayOutputDSteam"){};

        SingleOutputStreamOperator<String> splitDSteam = jsonObjIsNewDStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                        JSONObject startObj = jsonObject.getJSONObject("start");
                        if (startObj != null && startObj.size() > 0) {
                            // 如果是启动日志
                            context.output(startOutputDSteam, jsonObject.toJSONString());
                        } else {
                            // 如果不是启动日志，则一定是页面日志，输出到主流
                            collector.collect(jsonObject.toJSONString());

                            // 如果是曝光日志，输出到曝光测输出流
                            JSONArray displaysObjArr = jsonObject.getJSONArray("displays");
                            if (displaysObjArr != null && displaysObjArr.size() > 0) {

                                // 获取曝光曝光页面和时间
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                String ts = jsonObject.getString("ts");

                                for (int i = 0; i < displaysObjArr.size(); i++) {
                                    JSONObject obj = displaysObjArr.getJSONObject(i);
                                    // 为曝光信息增加曝光页面和曝光时间
                                    obj.put("page_id", pageId);
                                    obj.put("ts", ts);
                                    context.output(displayOutputDSteam, obj.toJSONString());
                                }
                            }
                        }
                    }
                }
        );

        DataStream<String> startDStream = splitDSteam.getSideOutput(new OutputTag<String>("startOutputDSteam") {});
        DataStream<String> displayDSteam = splitDSteam.getSideOutput(new OutputTag<String>("displayOutputDSteam") {});

        // splitDSteam.print("page>>>");
        // startDStream.print("start>>>");
        // displayDSteam.print("display>>>");

        // TODO 7.写入kafka不同topic
        splitDSteam.addSink(MyKafkaUtils.getKafkaSink("f_dwd_page_log"));
        startDStream.addSink(MyKafkaUtils.getKafkaSink("f_dwd_start_log"));
        displayDSteam.addSink(MyKafkaUtils.getKafkaSink("f_dwd_display_log"));

        env.execute();
    }
}
