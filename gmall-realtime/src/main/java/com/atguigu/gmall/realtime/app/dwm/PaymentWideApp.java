package com.atguigu.gmall.realtime.app.dwm;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.beans.PaymentInfo;
import com.atguigu.gmall.realtime.beans.PaymentWide;
import com.atguigu.gmall.realtime.utils.MyDateTimeUtils;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * 支付宽表
 * 启动应用：
 *      zk kf mxw hdfs Phoenix(hbase) redis BaseDBApp OrderWideApp PaymentWideApp
 */
public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.准备实时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点
        // TODO 3.从kafka读取数据
        // TODO 4.类型转换：jsonStr -> 实体类
        // TODO 5.设置事件时间字段
        // TODO 6.按照订单id分组
        // 支付
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDStream = env
                .addSource(MyKafkaUtils.getKafkaConsumer(DWD_PAYMENT_INFO, DWM_PAYMENT_WIDE_GROUP_ID))
                .map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<PaymentInfo>() {
                                            @Override
                                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                                return MyDateTimeUtils.toTs(element.getCallback_time());
                                            }
                                        }
                                )
                )
                .keyBy(PaymentInfo::getOrder_id);

        // 订单宽表
        KeyedStream<OrderWide, Long> orderWideKeyedDStream = env
                .addSource(MyKafkaUtils.getKafkaConsumer(DWM_ORDER_WIDE, DWM_PAYMENT_WIDE_GROUP_ID))
                .map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderWide>() {
                                            @Override
                                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                                return MyDateTimeUtils.toTs(element.getCreate_time());
                                            }
                                        }
                                )
                )
                .keyBy(OrderWide::getOrder_id);

        // TODO 6.双流join
        SingleOutputStreamOperator<PaymentWide> resDStream = paymentInfoKeyedDStream
                .intervalJoin(orderWideKeyedDStream)
                // 支付操作支付的是已有的订单，不能支付未来的订单
                .between(Time.minutes(-30), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        resDStream.print();

        // TODO 7.写入kafka
        resDStream
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtils.getKafkaProducer(DWM_PAYMENT_WIDE));

        env.execute();
    }
}
