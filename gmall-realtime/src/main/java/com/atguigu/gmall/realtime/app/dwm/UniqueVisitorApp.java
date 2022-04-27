package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.UniqueVisitorFilterFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        // GmallCheckPoint.getInstance().setCheckPoint(env);

        // TODO 3.从kafka主题：f_dwd_page_log 消费数据
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = env
                .addSource(MyKafkaUtils.getKafkaConsumer(DWD_PAGE_LOG, DWM_UNIQUE_VISITOR_GROUP_ID))
                // TODO 4.转换结构
                .map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDStream = jsonObjDStream
                // TODO 5.按照mid将数据分组
                .keyBy(r -> r.getJSONObject("common").getString("mid"))
                // TODO 6.过滤每天的UV：状态编程
                .filter(new UniqueVisitorFilterFunction());

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
