package com.atguigu.gmall.realtime.app.dws;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.VisitorStats;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtils;
import com.atguigu.gmall.realtime.utils.MyDateTimeUtils;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.Date;

/**
 * 访客主题
 * 启动应用
 * zk kf logger BaseLogApp UniqueVisitorApp UserJumpDetailApp VisitorStatsApp
 */
public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点

        // TODO 3.读取kafka数据
        DataStreamSource<String> pageLogKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_PAGE_LOG, DWS_VISITOR_STATS_GROUP_ID));
        DataStreamSource<String> uvKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWM_UNIQUE_VISITOR, DWS_VISITOR_STATS_GROUP_ID));
        DataStreamSource<String> ujdKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWM_USER_JUMP_DETAIL, DWS_VISITOR_STATS_GROUP_ID));

        // TODO 4.结构转换：jsonStr -> VisitorStats
        SingleOutputStreamOperator<VisitorStats> pageStatsDStream = pageLogKafkaDStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObject.getJSONObject("common");
                        JSONObject page = jsonObject.getJSONObject("page");
                        String lastPageId = page.getString("last_page_id");

                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                1L,
                                (lastPageId == null || lastPageId.length() == 0) ? 1L : 0L,
                                0L,
                                page.getLong("during_time"),
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> uvStatsDStream = uvKafkaDStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObject.getJSONObject("common");

                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> ujdStatsDSteam = ujdKafkaDStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObject.getJSONObject("common");

                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        //pageStatsDStream.print("page>>>");
        //uvStatsDStream.print("uv>>>");
        //ujdStatsDSteam.print("ujd>>>");

        // TODO 5.union
        SingleOutputStreamOperator<VisitorStats> resDSteam = pageStatsDStream
                .union(uvStatsDStream, ujdStatsDSteam)
                // TODO 6.设置水位线，指定事件时间字段
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<VisitorStats>() {
                                            @Override
                                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                )
                // TODO 7.分组。使用维度进行分组
                // 注意：不能使用mid进行分组
                // 原因1：单位窗口时间内，使用mid进行分组，起不到很好的聚合效果
                // 原因2：使用mid进行分组，后面在聚合的时候，会覆盖掉维度
                .keyBy(
                        new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                            @Override
                            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                                return Tuple4.of(
                                        visitorStats.getVc(),
                                        visitorStats.getCh(),
                                        visitorStats.getAr(),
                                        visitorStats.getIs_new()
                                );
                            }
                        }
                )
                // TODO 8.开窗
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                                return value1;
                            }
                        },
                        new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> tuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> out) throws Exception {
                                for (VisitorStats visitorStats : iterable) {
                                    visitorStats.setStt(MyDateTimeUtils.toDate(new Date(timeWindow.getStart())));
                                    visitorStats.setEdt(MyDateTimeUtils.toDate(new Date(timeWindow.getEnd())));
                                    visitorStats.setTs(System.currentTimeMillis());
                                    out.collect(visitorStats);
                                }
                            }
                        }
                );

        resDSteam.print("resDSteam>>>");

        // TODO 9.写入ClickHouse
        String sql = "insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)";
        resDSteam.addSink(MyClickHouseUtils.getSink(sql));

        env.execute();
    }
}
