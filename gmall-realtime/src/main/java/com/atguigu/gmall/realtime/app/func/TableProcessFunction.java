package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.utils.MyPhoenixUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.atguigu.gmall.realtime.common.GamllConfig.*;

/**
 * dwd层分流
 * 事实表：kafka
 * 维度表：hbase
 * <p>
 * processBroadcastElement和processElement的执行的时候不区分先后顺序，但实际执行时，需要先获取广播流数据，读取配置表信息
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> hbaseTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection conn;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = MyPhoenixUtils.getConnection();
    }

    /**
     * 处理广播流数据
     *
     * @param in FlinkCDC读取配置表json格式的字符串数据
     *           {
     *           "data": {
     *           "operate_type": "update",
     *           "sink_type": "hbase",
     *           "sink_table": "dim_base_trademark",
     *           "source_table": "base_trademark",
     *           "sink_extend": "",
     *           "sink_pk": "id,name",
     *           "sink_columns": "id,tm_name,age"
     *           }
     *           }
     */
    @Override
    public void processBroadcastElement(String in, Context context, Collector<JSONObject> out) throws Exception {

        // 解析数据
        JSONObject jsonObject = JSON.parseObject(in);
        JSONObject data = jsonObject.getJSONObject("data");
        // 将配置信息转换为TableProcess对象
        TableProcess tableProcess = data.toJavaObject(TableProcess.class);

        // 获取字段信息
        String sourceTable = tableProcess.getSourceTable();
        String sinkType = tableProcess.getSinkType();
        String operateType = tableProcess.getOperateType();
        String sinkTable = tableProcess.getSinkTable();
        String sinkColumns = tableProcess.getSinkColumns();
        String sinkPk = tableProcess.getSinkPk();
        String sinkExtend = tableProcess.getSinkExtend();

        // 创建纬度表: 对于写入hbase的数据，并且是insert操作
        if (SINK_TYPE_HBASE.equals(sinkType) && PHOENIX_OP_INSERT.equals(operateType)) {
            createTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        // 获取广播状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // 写入状态，进行广播
        String key = sourceTable + "-" + operateType;
        broadcastState.put(key, tableProcess);
    }

    /**
     * 处理业务流数据
     *
     * @param in 业务流输入数据
     */
    @Override
    public void processElement(JSONObject in, ReadOnlyContext context, Collector<JSONObject> out) throws Exception {

        // 读取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String table = in.getString("table");
        String type = in.getString("type");

        // 如果是maxwell通过bootstrap的方式获取历史数据，需要将bootstrap-insert转换为insert
        if (PHOENIX_OP_BOOTSTRAP_INSERT.equals(type)) {
            type = "insert";
        }

        String key = table + "-" + type;
        // 根据key读取配置信息
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            // 根据配置表中的sinkColumns字段过滤主流数据
            JSONObject data = in.getJSONObject("data");
            String columns = tableProcess.getSinkColumns();
            filterColumns(data, columns);

            // 将写出路径添加到tableProcess
            in.put("sink_table", tableProcess.getSinkTable());

            // key对应的配置信息存在，进行分流操作
            // 获取写出路径类型: kafka/hbase
            String sinkType = tableProcess.getSinkType();
            if (SINK_TYPE_KAFKA.equals(sinkType)) {
                // 主流：事实表：kafka
                out.collect(in);
            } else if (SINK_TYPE_HBASE.equals(sinkType)) {
                // 侧输出流：维度表：hbase
                context.output(hbaseTag, in);
            }
        } else {
            // key对应的配置信息不存在
            System.err.println("key " + key + " don't exists.");
        }
    }

    /**
     * 在hbase创建表
     *
     * @param sinkTable   目标表
     * @param sinkColumns 目标字段
     * @param sinkPk      主键信息
     * @param sinkExtend  扩展信息
     */
    private void createTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        // create table if not exists 表空间.table(
        //      id varchar primary key,
        //      name varchar
        // ) ext;

        // 添加默认主键
        if (sinkPk == null) {
            sinkPk = "id";
        }

        // 若不存在扩展字段，拼接空串
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        StringBuilder createSql = new StringBuilder();
        createSql
                .append("create table if not exists ")
                .append(HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");
        // 拼接字段
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            if (field.equals(sinkPk)) {
                // 主键字段
                createSql.append(field).append(" varchar primary key");
            } else {
                // 非主键字段
                createSql.append(field).append(" varchar");
            }

            // 最后一个字段后不拼接","
            if (i != fields.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")").append(sinkExtend);

        // 打印SQL
        System.out.println("createSql: " + createSql);

        PreparedStatement ps = null;
        try {
            // 获取操作对象
            ps = conn.prepareStatement(createSql.toString());
            // 执行SQL
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("");
        } finally {
            MyPhoenixUtils.close(ps);
        }
    }

    /**
     * 根据columns字段过滤主流数据
     *
     * @param data    主流数据            "data":{"tm_name":"LV","logo_url":"/home","id":13}
     * @param columns 配置表字段          id,tm_name
     */
    private void filterColumns(JSONObject data, String columns) {

        // 配置表字段
        List<String> columnList = Arrays.asList(columns.split(","));

        /*Iterator<String> it = data.keySet().iterator();
        while (it.hasNext()) {
            String next = it.next();
            if (!columnList.contains(next)) {
                it.remove();
            }
        }*/

        data.entrySet().removeIf(kv -> !columnList.contains(kv.getKey()));
    }
}
