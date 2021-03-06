package com.atguigu.gmall.realtime.app.dws;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;
import com.atguigu.gmall.realtime.beans.ProvinceStats;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtils;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 地区主题统计
 * 启动应用
 * zk kf mxw hdfs hbase redis clickhouse BaseDBApp OrderWideApp ProvinceStatsApp
 */
public class ProvinceStatsApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.检查点相关设置(略)
        // TODO 3.从Kafka中读取数据 创建动态表
        // 与kafka字段保持一致
        tableEnv.executeSql(
                "CREATE TABLE order_wide (" +
                        "province_id BIGINT," +
                        "province_name STRING," +
                        "province_area_code STRING," +
                        "province_iso_code STRING," +
                        "province_3166_2_code STRING," +
                        "order_id STRING," +
                        "split_total_amount DOUBLE," +
                        "create_time STRING," +
                        "rowtime as TO_TIMESTAMP(create_time)," +
                        "WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND" +
                        ") WITH (" + MyKafkaUtils.getKafkaDDL(DWM_ORDER_WIDE, DWS_PROVINCE_STATS_GROUP_ID) + ")"
        );

        // TODO 4.从动态表中查询数据   分组、开窗、聚合计算
        // 与bean对象字段名一致
        Table table = tableEnv.sqlQuery(
                "select " +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt," +
                        "province_id," +
                        "province_name," +
                        "province_area_code area_code," +
                        "province_iso_code iso_code," +
                        "province_3166_2_code iso_3166_2," +
                        "count(distinct order_id) order_count," +
                        "sum(split_total_amount) order_amount," +
                        "UNIX_TIMESTAMP() * 1000 ts " +
                        "from " +
                        "order_wide " +
                        "group by " +
                        "TUMBLE(rowtime, INTERVAL '10' SECOND)," +
                        "province_id, province_name, province_area_code, province_iso_code, province_3166_2_code"
        );

        // TODO 5.将动态表转换为流
        DataStream<ProvinceStats> provinceDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        provinceDS.print();

        // TODO 6.将流中数据写到ClickHouse中
        String sql = "insert into province_stats values(?,?,?,?,?,?,?,?,?,?)";
        provinceDS.addSink(MyClickHouseUtils.getSink(sql));

        env.execute();
    }
}
