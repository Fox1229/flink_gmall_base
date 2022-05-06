package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.OrderDetail;
import com.atguigu.gmall.realtime.beans.OrderInfo;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.utils.MyDateTimeUtils;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * 订单宽表
 * 启动应用：
 * zk kf mxw hdfs hbase(Phoenix) BaseDBApp OrderWideApp
 * 维度关联
 *      基本维度关联实现
 *          PhoenixUtil
 *              List<T> queryList(String sql,Class<T> clz)
 *          DimUtil
 *              JSONObject getDimInfoNoCache(String tableName,Tuple2<String,String> ... params);
 *      优化1：旁路缓存
 *          JSONObject getDimInfo(String tableName,Tuple2<String,String> ... params);
 *      优化2：异步IO
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

        // orderWideDStream.print(">>>");

        // TODO 8.纬度关联
        /*orderWideDStream.map(
                new MapFunction<OrderWide, OrderWide>() {
                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {

                        // 获取要关联的用户维度的id
                        String userId = orderWide.getOrder_id().toString();

                        // 获取纬度信息
                        JSONObject jsonObject = DimUtil.getDimInfo(PHOENIX_DIM_USER_INFO, userId);
                        if (jsonObject != null) {
                            // 注意：字段名大写
                            String gender = jsonObject.getString("GENDER");
                            String birthday = jsonObject.getString("BIRTHDAY");
                            LocalDate birthdayLd = LocalDate.parse(birthday);
                            LocalDate nowLd = LocalDate.now();
                            int age = Period.between(birthdayLd, nowLd).getYears();

                            System.out.println("age: " + birthdayLd + " " + nowLd + " " + age);

                            orderWide.setUser_gender(gender);
                            orderWide.setUser_age(age);
                        }

                        return orderWide;
                    }
                }
        );*/

        // 用户纬度关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDStream = AsyncDataStream.unorderedWait(
                orderWideDStream,
                new DimAsyncFunction<OrderWide>(PHOENIX_DIM_USER_INFO) {

                    @Override
                    public String getPrimaryKey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        // 注意：字段名大写
                        String gender = jsonObject.getString("GENDER");
                        String birthday = jsonObject.getString("BIRTHDAY");

                        orderWide.setUser_gender(gender);
                        orderWide.setUser_age(MyDateTimeUtils.getAge(birthday));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 地区维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithBaseProvinceDStream = AsyncDataStream.unorderedWait(
                orderWideWithUserInfoDStream,
                new DimAsyncFunction<OrderWide>(PHOENIX_DIM_BASE_PROVINCE) {

                    @Override
                    public String getPrimaryKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {

                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 商品维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuInfoDStream = AsyncDataStream.unorderedWait(
                orderWideWithBaseProvinceDStream,
                new DimAsyncFunction<OrderWide>(PHOENIX_DIM_SKU_INFO) {

                    @Override
                    public String getPrimaryKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // SPU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuInfoDStream = AsyncDataStream.unorderedWait(
                orderWideWithSkuInfoDStream,
                new DimAsyncFunction<OrderWide>(PHOENIX_DIM_SPU_INFO) {

                    @Override
                    public String getPrimaryKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 类别维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DStream = AsyncDataStream.unorderedWait(
                orderWideWithSpuInfoDStream,
                new DimAsyncFunction<OrderWide>(PHOENIX_DIM_BASE_CATEGORY3) {

                    @Override
                    public String getPrimaryKey(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setCategory3_name(dimInfo.getString("NAME"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 品牌维度进行关联
        SingleOutputStreamOperator<OrderWide> resDStream = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DStream,
                new DimAsyncFunction<OrderWide>(PHOENIX_DIM_BASE_TRADEMARK) {

                    @Override
                    public String getPrimaryKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 打印
        resDStream.print();

        //TODO 9.将关联后的宽表写到kafka的dwm_order_wide主题中
        resDStream
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtils.getKafkaProducer(DWM_ORDER_WIDE));

        env.execute();
    }
}
