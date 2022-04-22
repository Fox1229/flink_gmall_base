package com.atguigu.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import static com.atguigu.gmall.realtime.common.GamllConfig.*;
import java.text.SimpleDateFormat;

/**
 * 日志数据分流
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，与kafka分区数保持一致
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.检查点相关设置
        // 开启检查点：精准一次消费
        // 检查点不对齐：重复数据
        /*env.enableCheckpointing(5 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        // 检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 取消job是否保留检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2 * 1000L);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend(HDFS_CHECKPOINT_PATH));
        // 设置操作hdfs的用户
        System.setProperty(HADOOP_USER_KEY, HADOOP_USER_NAME);
        // 设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 30天一个周期，允许失败次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));*/

        // TODO 3.从kafka主题消费数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtils.getKafkaSource(ODS_BASE_LOG, ODS_BASE_LOG_GROUP_ID);
        DataStreamSource<String> kafkaDStream = env.addSource(kafkaSource);

        // TODO 4.将数据转化为json对象
        // 补充：可以考虑使用process将解析失败的数据写入侧输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = kafkaDStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String logStr) throws Exception {
                        return JSON.parseObject(logStr);
                    }
                }
        );

        // TODO 5.对新老访客进行修复：状态编程
        // 卸载重装后is_new字段可能会出问题
        SingleOutputStreamOperator<JSONObject> jsonObjIsNewDStream = jsonObjDStream
                // 按照数据mid将数据进行分组
                .keyBy(r -> r.getJSONObject("common").get("mid"))
                // 判断设备是否首次登录，对is_new字段进行更新
                // 0：非首次登录，不做处理
                // 1：检查状态中是否有当前mid的登录信息，如果有则不是首次登录，将状态置为0；否则为首次登录，记录mid的登录时间
                .map(
                        new RichMapFunction<JSONObject, JSONObject>() {

                            // 维护用户登录状态
                            // 不能在声明的时候直接对状态进行初始化，因为生命周期还没有开始，获取不到RuntimeContext
                            private ValueState<String> lastVisitDateState;
                            // 格式化日期
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

                                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                                // 修复状态
                                if ("1".equals(isNew)) {

                                    String lastVisitDate = lastVisitDateState.value();
                                    String currentVisitDate = sdf.format(jsonObject.getLong("ts"));

                                    if (lastVisitDate != null && lastVisitDate.length() != 0) {
                                        // 存在该mid的状态，非首次登录
                                        // 同一天多次登录不做更新
                                        if (!currentVisitDate.equals(lastVisitDate)) {
                                            isNew = "0";
                                            jsonObject.getJSONObject("common").put("is_new", isNew);
                                        }
                                    } else {
                                        // 不存在mid的，首次登录，维护状态
                                        lastVisitDateState.update(currentVisitDate);
                                    }
                                }

                                return jsonObject;
                            }
                        }
                );

        // TODO 6.对日志数据进行分流：测输出流
        // 定义测输出流标签
        OutputTag<String> startJsonOutput = new OutputTag<String>("startJsonObj") {};
        OutputTag<String> displayJsonOutput = new OutputTag<String>("displayJsonObj") {};

        SingleOutputStreamOperator<String> splitDStream = jsonObjIsNewDStream.process(
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> out) throws Exception {

                        JSONObject startState = jsonObject.getJSONObject("start");

                        if (startState != null && startState.size() > 0) {
                            // 启动日志
                            context.output(startJsonOutput, jsonObject.toJSONString());
                        } else {
                            // 如果不是启动日志，则属于页面日志，放到主流
                            out.collect(jsonObject.toJSONString());

                            // 包含曝光信息即为曝光日志，测输出到曝光流
                            JSONArray displaysJsonArr = jsonObject.getJSONArray("displays");
                            if (displaysJsonArr != null && displaysJsonArr.size() != 0) {

                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                String ts = jsonObject.getString("ts");
                                for (int i = 0; i < displaysJsonArr.size(); i++) {
                                    JSONObject jsonObj = displaysJsonArr.getJSONObject(i);
                                    // 为每条曝光信息添加曝光页面和曝光时间
                                    jsonObj.put("page_id", pageId);
                                    jsonObj.put("ts", ts);
                                    context.output(displayJsonOutput, jsonObj.toJSONString());
                                }
                            }
                        }
                    }
                }
        );

        // 获取测输出流
        DataStream<String> startStream = splitDStream.getSideOutput(startJsonOutput);
        DataStream<String> displayStream = splitDStream.getSideOutput(displayJsonOutput);

        // splitDStream.print("page>>>");
        // startStream.print("start$$$");
        // displayStream.print("display###");

        // TODO 7.将分流后的数据写入kafka的不同主题
        splitDStream.addSink(MyKafkaUtils.getKafkaSink(DWD_PAGE_LOG));
        startStream.addSink(MyKafkaUtils.getKafkaSink(DWD_START_LOG));
        displayStream.addSink(MyKafkaUtils.getKafkaSink(DWD_DISPLAY_LOG));

        env.execute();
    }
}
