package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallCheckPoint;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * 独立访客
 * 启动服务：nginx logger zk kf BaseLogApp
 */
public class UniqueVisitorApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.准备实时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置分区数
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点
        GmallCheckPoint.getInstance().setCheckPoint(env);

        // TODO 3.从kafka主题：f_dwd_page_log 消费数据
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = env
                .addSource(MyKafkaUtils.getKafkaSource(DWD_PAGE_LOG, DWD_PAGE_LOG_GROUP_ID))
                // TODO 4.转换结构
                .map(JSON::parseObject);

        // jsonObjDStream.print();

        SingleOutputStreamOperator<JSONObject> filterDStream = jsonObjDStream
                // TODO 5.按照mid将数据分组
                .keyBy(r -> r.getJSONObject("common").getString("mid"))
                // TODO 6.过滤每天的UV：状态编程
                .filter(
                        new RichFilterFunction<JSONObject>() {

                            // mid上一次登录时间
                            private ValueState<String> lastVisitDateState;
                            private SimpleDateFormat sdf;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDate", Types.STRING);
                                // 设置状态生命周期
                                valueStateDescriptor.enableTimeToLive(
                                        StateTtlConfig
                                                .newBuilder(Time.days(1))
                                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                                .build()
                                );
                                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                                sdf = new SimpleDateFormat("yyyyMMdd");
                            }

                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {

                                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
                                // 如果当前页面是从其它页面跳转过来的，说明这次访问就不属于独立访客了，直接过滤掉
                                if (lastPage != null && lastPage.length() > 0) {
                                    return false;
                                }

                                // 如果今日存在登录状态，则过滤
                                String lastDate = lastVisitDateState.value();
                                // 获取设备访问时间
                                String currentDate = sdf.format(jsonObject.getLong("ts"));
                                if (lastDate != null && lastDate.length() > 0 && lastDate.equals(currentDate)) {
                                    // 设备在今日访问过
                                    return false;
                                } else {
                                    // 设备没有在今日访问过
                                    // 更新状态
                                    lastVisitDateState.update(currentDate);
                                    return true;
                                }
                            }
                        }
                );

        // 打印
        filterDStream.print("filter>>>");

        // TODO 6.将数据写入kafka
        filterDStream
                // 转换类型
                .map(JSON::toString)
                .addSink(MyKafkaUtils.getKafkaProducer(DWM_UNIQUE_VISITOR));

        env.execute();
    }
}
