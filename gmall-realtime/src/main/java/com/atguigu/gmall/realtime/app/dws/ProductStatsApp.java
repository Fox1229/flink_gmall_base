package com.atguigu.gmall.realtime.app.dws;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;
import static com.atguigu.gmall.realtime.common.GmallConstant.APPRAISE_GOOD;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.beans.PaymentWide;
import com.atguigu.gmall.realtime.beans.ProductStats;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtils;
import com.atguigu.gmall.realtime.utils.MyDateTimeUtils;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 上坪主题计算
 * <p>
 * 启动进程：
 * zk kf logger maxwell hdfs hbase redis clickhouse
 * BaseLogApp BaseDBApp OrderWideApp PaymentWideApp ProductStatsApp
 */
public class ProductStatsApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);

        // TODO 2.设置检查点
        /*env.enableCheckpointing(CHECKPOINT_PERIOD, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CHECKPOINT_MIN_BETWEEN);
        // 设置检查点重启策略
        env.setRestartStrategy(new RestartStrategies.FailureRateRestartStrategyConfiguration(FAIL_RATE, Time.days(30), Time.seconds(3)));
        env.setStateBackend(new FsStateBackend(HDFS_CHECKPOINT_PATH));
        System.setProperty(HADOOP_USER_KEY, HADOOP_USER_NAME);*/

        // TODO 3.从kafka读取数据
        DataStreamSource<String> clickAndDisplayKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_PAGE_LOG, DWS_PRODUCT_STATS_GROUP_ID));
        DataStreamSource<String> favorInfoKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_FAVOR_INFO, DWS_PRODUCT_STATS_GROUP_ID));
        DataStreamSource<String> cartInfoKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_CART_INFO, DWS_PRODUCT_STATS_GROUP_ID));
        DataStreamSource<String> orderWideKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWM_ORDER_WIDE, DWS_PRODUCT_STATS_GROUP_ID));
        DataStreamSource<String> paymentInfoKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWM_PAYMENT_WIDE, DWS_PRODUCT_STATS_GROUP_ID));
        DataStreamSource<String> refundPaymentKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWM_ORDER_REFUND_INFO, DWS_PRODUCT_STATS_GROUP_ID));
        DataStreamSource<String> commentInfoKafkaDStream = env.addSource(MyKafkaUtils.getKafkaConsumer(DWD_COMMENT_INFO, DWS_PRODUCT_STATS_GROUP_ID));

        // TODO 4.类型转换：jsonStr -> ProductStats
        // 点击、曝光
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDStream = clickAndDisplayKafkaDStream.process(
                new ProcessFunction<String, ProductStats>() {

                    @Override
                    public void processElement(String jsonStr, Context context, Collector<ProductStats> out) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        JSONObject pageObj = jsonObject.getJSONObject("page");
                        Long ts = jsonObject.getLong("ts");
                        // 商品点击行为
                        // 如果当前页面的id是good_detail，那么这次页面日志才是商品的点击行为
                        if ("good_detail".equals(pageObj.getString("page_id"))) {
                            ProductStats productStats = ProductStats
                                    .builder()
                                    .sku_id(pageObj.getLong("item"))
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }

                        // 商品曝光行为
                        JSONArray jsonArray = jsonObject.getJSONArray("displays");
                        if (jsonArray != null && jsonArray.size() > 0) {
                            // 遍历每一个曝光记录
                            for (int i = 0; i < jsonArray.size(); i++) {
                                JSONObject obj = jsonArray.getJSONObject(i);
                                // 判断是否是商品曝光行为
                                if ("sku_id".equals(obj.getString("item_type"))) {
                                    ProductStats productStats = ProductStats
                                            .builder()
                                            .sku_id(obj.getLong("item"))
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build();
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );

        // 收藏
        SingleOutputStreamOperator<ProductStats> favorStatsDStream = favorInfoKafkaDStream.map(
                new MapFunction<String, ProductStats>() {

                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        return ProductStats
                                .builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(MyDateTimeUtils.toTs(jsonObject.getString("create_time")))
                                .build();
                    }
                }
        );

        // 加购
        SingleOutputStreamOperator<ProductStats> cartStatsDStream = cartInfoKafkaDStream.map(
                new MapFunction<String, ProductStats>() {

                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        return ProductStats
                                .builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(MyDateTimeUtils.toTs(jsonObject.getString("create_time")))
                                .build();
                    }
                }
        );

        // 下单
        SingleOutputStreamOperator<ProductStats> orderStatsDStream = orderWideKafkaDStream.map(
                new MapFunction<String, ProductStats>() {

                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        // 转化为订单宽表对象
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                        return ProductStats
                                .builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .ts(MyDateTimeUtils.toTs(orderWide.getCreate_time()))
                                .build();
                    }
                }
        );

        // 支付
        SingleOutputStreamOperator<ProductStats> paymentStatsDStream = paymentInfoKafkaDStream.map(
                new MapFunction<String, ProductStats>() {

                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                        return ProductStats
                                .builder()
                                .sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                                .ts(MyDateTimeUtils.toTs(paymentWide.getCallback_time()))
                                .build();
                    }
                }
        );

        // 退单
        SingleOutputStreamOperator<ProductStats> refundStatsDStream = refundPaymentKafkaDStream.map(
                new MapFunction<String, ProductStats>() {

                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        return ProductStats
                                .builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                                .refundOrderIdSet(new HashSet(Collections.singleton(jsonObject.getLong("order_id"))))
                                .ts(MyDateTimeUtils.toTs(jsonObject.getString("create_time")))
                                .build();
                    }
                }
        );

        // 评论
        SingleOutputStreamOperator<ProductStats> commentStatsDStream = commentInfoKafkaDStream.map(
                new MapFunction<String, ProductStats>() {

                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        // 好评数
                        long cnt = APPRAISE_GOOD.equals(jsonObject.getString("appraise")) ? 1L : 0L;
                        return ProductStats
                                .builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .comment_ct(1L)
                                .good_comment_ct(cnt)
                                .ts(MyDateTimeUtils.toTs(jsonObject.getString("create_time")))
                                .build();
                    }
                }
        );

        // TODO 5.union
        SingleOutputStreamOperator<ProductStats> reduceStatsDStream = clickAndDisplayStatsDStream
                .union(
                        favorStatsDStream,
                        cartStatsDStream,
                        orderStatsDStream,
                        paymentStatsDStream,
                        refundStatsDStream,
                        commentStatsDStream
                )
                // TODO 6.指定WaterMark和事件时间字段
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<ProductStats>() {
                                            @Override
                                            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                )
                // TODO 7.使用sku_id分组
                .keyBy(ProductStats::getSku_id)
                // TODO 8.开窗
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // TODO 9.聚合计算
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats v1, ProductStats v2) throws Exception {

                                v1.setClick_ct(v1.getClick_ct() + v2.getClick_ct());
                                v1.setDisplay_ct(v1.getDisplay_ct() + v2.getDisplay_ct());
                                v1.setCart_ct(v1.getCart_ct() + v2.getCart_ct());
                                v1.setFavor_ct(v1.getFavor_ct() + v2.getFavor_ct());

                                // 下单
                                v1.setOrder_amount(v1.getOrder_amount().add(v2.getOrder_amount()));
                                v1.getOrderIdSet().addAll(v2.getOrderIdSet());
                                v1.setOrder_ct((long) v1.getOrderIdSet().size());
                                v1.setOrder_sku_num(v1.getOrder_sku_num() + v2.getOrder_sku_num());

                                // 支付
                                v1.setPayment_amount(v1.getPayment_amount().add(v2.getPayment_amount()));
                                v1.getPaidOrderIdSet().addAll(v2.getPaidOrderIdSet());
                                v1.setPaid_order_ct((long) v1.getPaidOrderIdSet().size());

                                // 退单
                                v1.setRefund_amount(v1.getRefund_amount().add(v2.getRefund_amount()));
                                v1.getRefundOrderIdSet().addAll(v2.getRefundOrderIdSet());
                                v1.setRefund_order_ct((long) v1.getRefundOrderIdSet().size());

                                // 评论
                                v1.setComment_ct(v1.getComment_ct() + v2.getComment_ct());
                                v1.setGood_comment_ct(v1.getGood_comment_ct() + v2.getGood_comment_ct());

                                return v1;
                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {

                            @Override
                            public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> out) throws Exception {

                                // 更新开始、结束时间和事件时间字段
                                for (ProductStats productStats : iterable) {
                                    productStats.setStt(MyDateTimeUtils.toDate(new Date(timeWindow.getStart())));
                                    productStats.setEdt(MyDateTimeUtils.toDate(new Date(timeWindow.getEnd())));
                                    productStats.setTs(System.currentTimeMillis());
                                    out.collect(productStats);
                                }
                            }
                        }
                );

        // TODO 10.纬度关联
        // 和商品纬度sku进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceStatsDStream,
                new DimAsyncFunction<ProductStats>(PHOENIX_DIM_SKU_INFO) {
                    @Override
                    public String getPrimaryKey(ProductStats obj) {
                        return obj.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) {
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 和商品spu纬度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuInfoDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuInfoDS,
                new DimAsyncFunction<ProductStats>(PHOENIX_DIM_SPU_INFO) {

                    @Override
                    public String getPrimaryKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) {
                        productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 和商品品类纬度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(
                productStatsWithSpuInfoDS,
                new DimAsyncFunction<ProductStats>(PHOENIX_DIM_BASE_CATEGORY3) {

                    @Override
                    public String getPrimaryKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) {
                        productStats.setCategory3_name(dimInfo.getString("NAME"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        // 和商品品牌纬度进行关联
        SingleOutputStreamOperator<ProductStats> resDS = AsyncDataStream.unorderedWait(
                productStatsWithCategory3DS,
                new DimAsyncFunction<ProductStats>(PHOENIX_DIM_BASE_TRADEMARK) {

                    @Override
                    public String getPrimaryKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) {
                        productStats.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                },
                ASYNC_REQUEST_TIMEOUT,
                TimeUnit.SECONDS
        );

        resDS.print();

        // TODO 11.写入ClickHouse
        String sql = "insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        resDS.addSink(MyClickHouseUtils.getSink(sql));

        env.execute();
    }
}
