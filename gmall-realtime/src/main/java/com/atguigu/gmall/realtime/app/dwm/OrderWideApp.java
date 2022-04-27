package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.beans.OrderDetail;
import com.atguigu.gmall.realtime.beans.OrderInfo;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * 订单宽表
 * 启动应用：
 * zk kf mxw hdfs hbase(Phoenix) BaseDBApp OrderWideApp
 */
public class OrderWideApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点

        // TODO 3.从kafka读取数据
        // orderInfo
        DataStreamSource<String> kafkaOrderInfoDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_ORDER_INFO, DWM_ORDER_WIDE_GROUP_ID));

        // orderDetail
        DataStreamSource<String> kafkaOrderDetailDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_ORDER_DETAIL, DWM_ORDER_WIDE_GROUP_ID));

        // TODO 4.格式转换：jsonStr -> 实体类
        SingleOutputStreamOperator<OrderInfo> orderInfoDStream = kafkaOrderInfoDStream.map(
                new RichMapFunction<String, OrderInfo>() {

                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String value) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                        // 2022-04-20 20:39:21
                        String createTime = orderInfo.getCreate_time();
                        String[] fields = createTime.split(" ");
                        String createDate = fields[0];
                        String createHour = fields[1].split(":")[0];
                        orderInfo.setCreate_date(createDate);
                        orderInfo.setCreate_hour(createHour);

                        long createTs = sdf.parse(createTime).getTime();
                        orderInfo.setCreate_ts(createTs);

                        return orderInfo;
                    }
                }
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailDStream = kafkaOrderDetailDStream.map(
                new RichMapFunction<String, OrderDetail>() {

                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String value) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                        // 2022-04-20 20:39:21
                        String createTime = orderDetail.getCreate_time();
                        long createTs = sdf.parse(createTime).getTime();
                        orderDetail.setCreate_ts(createTs);

                        return orderDetail;
                    }
                }
        );

        // 打印
        // orderInfoDStream.print();
        // orderDetailDStream.print();

        // TODO 5.设置水位线字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDStream = orderInfoDStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDStream = orderDetailDStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                }
                        )
        );

        // TODO 6.按照订单id对订单和订单明细进行分组：指定两条流关联字段
        // orderInfo
        KeyedStream<OrderInfo, Long> orderInfoKeyedDStream = orderInfoWithWatermarkDStream.keyBy(OrderInfo::getId);

        // orderDetail
        KeyedStream<OrderDetail, Long> orderDetailKeyedDStream = orderDetailWithWatermarkDStream.keyBy(OrderDetail::getOrder_id);

        // TODO 7.使用基于状态双流join实现：intervaljoin将订单和明细关联在一起
        SingleOutputStreamOperator<OrderWide> orderWideDStream = orderInfoKeyedDStream
                .intervalJoin(orderDetailKeyedDStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

        // orderWideDStream.print("orderWide>>>");

        // TODO 8.和用户维度进行关联
        // TODO 9.和地区维度进行关联
        // TODO 10.和商品维度进行关联
        // TODO 11.和SPU维度进行关联
        // TODO 12.和类别维度进行关联
        // TODO 13.和品牌维度进行关联

        //TODO 14.将关联后的宽表写到kafka的dwm_order_wide主题中


        env.execute();
    }
}
