package com.atguigu.gmall.realtime.utils;

import org.apache.commons.beanutils.BeanUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * Phoenix工具类
 */
public class MyPhoenixUtils {

    private static Connection conn;

    /*public static void main(String[] args) {

        System.out.println(queryList("select * from dim_base_trademark", JSONObject.class));
    }*/

    /**
     * 查询Phoenix表数据
     *
     * @param sql 查询的SQL语句
     * @param clz 查询结果的类型
     * @return 多个对象组成的集合
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {

        // 获取连接
        if (conn == null) {
            getConnection();
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        // 封装查询结果的集合
        ArrayList<T> list = new ArrayList<>();

        try {

            // 设置schema
            conn.setSchema(HBASE_SCHEMA);

            // 获取操作数据库对象
            ps = conn.prepareStatement(sql);

            // 执行查询
            rs = ps.executeQuery();

            // 获取查询表的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {

                T instance = clz.newInstance();

                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object columnValue = rs.getObject(columnName);
                    BeanUtils.setProperty(instance, columnName, columnValue);
                }

                list.add(instance);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            close(ps, rs);
        }

        return list;
    }

    /**
     * 获取Phoenix连接
     */
    public static Connection getConnection() {

        try {
            Class.forName(PHOENIX_DRIVER);
            conn = DriverManager.getConnection(PHOENIX_SERVER);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 释放资源
     */
    public static void close(PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(PreparedStatement ps, ResultSet rs) {

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
