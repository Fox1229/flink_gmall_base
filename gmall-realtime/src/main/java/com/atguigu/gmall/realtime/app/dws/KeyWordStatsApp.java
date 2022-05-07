package com.atguigu.gmall.realtime.app.dws;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;
import com.atguigu.gmall.realtime.app.func.KeyWordUDTF;
import com.atguigu.gmall.realtime.beans.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtils;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 关键词
 * 启动应用
 * zk kf logger clickhouse BaseLogApp KeyWordStatsApp
 */
public class KeyWordStatsApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(PARALLELISM_NUM);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.检查点设置

        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);

        // TODO 3.从kafka读取数据，创建动态表，指定watermark并提取事件时间字段
        tableEnv.executeSql(
                "CREATE TABLE page_view (" +
                        "common MAP<STRING, STRING>," +
                        "page MAP<STRING, STRING>," +
                        "ts BIGINT," +
                        "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss'))," +
                        "WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND" +
                        ") WITH (" + MyKafkaUtils.getKafkaDDL(DWD_PAGE_LOG, DWS_KEYWORD_STATS_GROUP_ID) + ")"
        );

        // TODO 4.过滤搜索行为
        Table fullWordView = tableEnv.sqlQuery(
                "select " +
                        "page['item'] full_word," +
                        "rowtime " +
                        "from " +
                        "page_view " +
                        "where page['page_id'] = 'good_list' and page['item'] is not null"
        );

        // TODO 5.使用分词函数对搜索内容进行分词，并将分好的词和原表字段进行连接
        Table keywordView = tableEnv.sqlQuery(
                "select keyword, rowtime from " + fullWordView + ", LATERAL TABLE(ik_analyze(full_word)) as t(keyword)"
        );

        // TODO 6.分组、开窗、聚合计算
        Table keywordStats = tableEnv.sqlQuery(
                "select " +
                        "keyword," +
                        "count(*) ct, '" +
                        GmallConstant.KEYWORD_SEARCH + "' source, " +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                        "UNIX_TIMESTAMP() * 1000 ts " +
                        "from " +
                        keywordView +
                        " group by " +
                        "TUMBLE(rowtime, INTERVAL '10' SECOND), keyword"
        );

        // TODO 7.将动态表转换为流
        DataStream<KeywordStats> resDSteam = tableEnv.toAppendStream(keywordStats, KeywordStats.class);

        resDSteam.print();

        // TODO 8.写入clickhouse
        String sql = "insert into keyword_stats(keyword, ct, source, stt, edt, ts) values(?,?,?,?,?,?)";
        resDSteam.addSink(MyClickHouseUtils.getSink(sql));

        env.execute();
    }
}
