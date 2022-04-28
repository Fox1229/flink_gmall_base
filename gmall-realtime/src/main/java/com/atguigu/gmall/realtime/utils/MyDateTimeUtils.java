package com.atguigu.gmall.realtime.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 日期转换工具类
 */
public class MyDateTimeUtils {

    private static final DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 计算年龄
     */
    public static Integer getAge(String birthday) {
        LocalDate birthdayLd = LocalDate.parse(birthday);
        LocalDate nowLd = LocalDate.now();
        return Period.between(birthdayLd, nowLd).getYears();
    }

    /**
     * 将时间转换为字符串
     */
    public static String toDate(Date date) {

        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return sdf.format(localDateTime);
    }

    /**
     * 将字符转换为毫秒数
     */
    public static Long toTs(String dataStr) {

        LocalDateTime dateTime = LocalDateTime.parse(dataStr, sdf);
        return dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
