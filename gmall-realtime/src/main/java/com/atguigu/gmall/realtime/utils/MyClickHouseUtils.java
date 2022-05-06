package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.beans.TransientSink;
import com.atguigu.gmall.realtime.beans.VisitorStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.atguigu.gmall.realtime.common.GmallConfig.CLICKHOUSE_DRIVER;
import static com.atguigu.gmall.realtime.common.GmallConfig.CLICKHOUSE_URL;

/**
 * 操作ClickHouse工具类
 */
public class MyClickHouseUtils {

    public static <T> SinkFunction <T> getSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {

                        // 获取当前对象的所有属性
                        Field[] fields = obj.getClass().getDeclaredFields();

                        int skipNum = 0;
                        for (int i = 0; i < fields.length; i++) {
                            // 获取属性名
                            Field field = fields[i];

                            // 保证当前属性是可访问的
                            field.setAccessible(true);

                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                skipNum++;
                                continue;
                            }

                            try {
                                // 获取属性值
                                Object fieldValue = field.get(obj);
                                // 给占位符赋值
                                ps.setObject(i + 1 - skipNum, fieldValue);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        // 单个并行度，写入匹次大小。默认5000。
                        .withBatchSize(5)
                        // 每n ms写入一次时间。默认0
                        // .withBatchIntervalMs()
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(CLICKHOUSE_DRIVER)
                        .withUrl(CLICKHOUSE_URL)
                        .build()
        );
    }
}
