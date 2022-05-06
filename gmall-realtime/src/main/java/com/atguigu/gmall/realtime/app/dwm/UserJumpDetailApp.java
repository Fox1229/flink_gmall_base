package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.List;
import java.util.Map;
import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * 用户跳出
 * 启动应用：
 *      zk kf logger BaseLogApp UserJumpDetailApp
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.读取实时数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点
        /*env.enableCheckpointing(CHECKPOINT_PERIOD, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CHECKPOINT_MIN_BETWEEN);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(HDFS_CHECKPOINT_PATH));
        System.setProperty(HADOOP_USER_KEY, HADOOP_USER_NAME);
        env.setRestartStrategy(new RestartStrategies.FailureRateRestartStrategyConfiguration(FAIL_RATE, Time.days(30), Time.seconds(3)));*/

        // TODO 3.从kafka读取数据：f_dwd_page_log
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = env
                .addSource(MyKafkaUtils.getKafkaConsumer(DWD_PAGE_LOG, DWM_USER_JUMP_DETAIL_GROUP_ID))
                // TODO 4.转换格式：jsonStr -> jsonObj
                .map(JSON::parseObject)
                // TODO 5.指定时间事件字段
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                                return jsonObject.getLong("ts");
                                            }
                                        }
                                )
                )
                // TODO 6.按照mid对数据分组
                .keyBy(r -> r.getJSONObject("common").getString("mid"));

        // TODO 7.使用CEP过滤超时数据
        // 定义pattern：取出跳转操作
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                                return lastPageId == null || lastPageId.length() == 0;
                            }
                        }
                )
                // 指定严格连续
                .next("next")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                return pageId != null && pageId.length() > 0;
                            }
                        }
                )
                // 当一个模式上通过within加上窗口长度后，部分匹配的事件序列就可能因为超过窗口长度而被"丢弃"。
                // 可以使用TimedOutPartialMatchHandler接口来处理超时的部分匹配。
                .within(Time.seconds(10));

        // 将pattern应用到数据流
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        // 取出超时数据
        // 定义测输出流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag") {};
        SingleOutputStreamOperator<String> resDStream = patternStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    // 超时数据
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> out) throws Exception {
                        List<JSONObject> jsonObjectList = map.get("start");
                        for (JSONObject jsonObject : jsonObjectList) {
                            // 将收集到的数据放到参数1的测输出流中
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    // 跳转数据
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {

                    }
                }
        );

        // 获取超时数据
        DataStream<String> sideOutput = resDStream.getSideOutput(timeoutTag);

        // 打印
        sideOutput.print("sideOutput>>>");

        // TODO 8.写入kafka
        sideOutput.addSink(MyKafkaUtils.getKafkaProducer(DWM_USER_JUMP_DETAIL));

        env.execute();
    }
}
