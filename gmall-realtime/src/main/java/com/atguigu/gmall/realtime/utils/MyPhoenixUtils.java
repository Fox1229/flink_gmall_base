package com.atguigu.gmall.realtime.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.atguigu.gmall.realtime.common.GamllConfig.PHOENIX_DRIVER;
import static com.atguigu.gmall.realtime.common.GamllConfig.PHOENIX_SERVER;

public class MyPhoenixUtils {

    /**
     * 获取Phoenix连接
     */
    public static Connection getConnection() throws Exception {
        Class.forName(PHOENIX_DRIVER);
        return DriverManager.getConnection(PHOENIX_SERVER);
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
}
