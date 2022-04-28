package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyDimUtils;
import com.atguigu.gmall.realtime.utils.MyPhoenixUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * dwd层纬度数据写入phoenix(hbase)
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取Phoenix连接
        conn = MyPhoenixUtils.getConnection();
    }

    /**
     *
     * @param in hbase流数据
     * {
     * 	"sink_table": "dim_base_trademark",
     * 	"data": {
     * 		"tm_name": "LV1",
     * 		"id": 13
     *        }
     * }
     */
    @Override
    public void invoke(JSONObject in, Context context) throws Exception {

        // 获取目标表
        String sinkTable = in.getString(SINK_TABLE_KEY);
        // 获取数据
        JSONObject data = in.getJSONObject("data");
        // 生成插入/更新数据的SQL
        String upsetSql = getUpsetSql(sinkTable, data);
        System.out.println("upsetSql: " + upsetSql);

        PreparedStatement ps = null;
        try {
            // 获取数据库操作对象
            ps = conn.prepareStatement(upsetSql);
            ps.executeUpdate();
            // Phoenix不会自动提交更新后的结果，需要手动提交
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            MyPhoenixUtils.close(ps);
        }

        // 如果是对维度表的更新操作，需要删除Redis缓存数据
        if (PHOENIX_OP_UPDATE.equals(in.getString("type"))) {
            MyDimUtils.deleteRedisCache(sinkTable, data.getString("id"));
        }
    }

    /**
     * 获取写入Phoenix
     * @param sinkTable 目标表
     * @param data 数据信息
     */
    private String getUpsetSql(String sinkTable, JSONObject data) {

        StringBuilder sb = new StringBuilder();
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();

        // upsert into 表空间.student(id, name, addr) values('1001','zhangsan','beijing');
        sb
                .append("upsert into ")
                .append(HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(")
                .append(StringUtils.join(keys, ","))
                .append(")")
                .append(" values")
                .append("('")
                .append(StringUtils.join(values, "','"))
                .append("')");

        return sb.toString();
    }
}
