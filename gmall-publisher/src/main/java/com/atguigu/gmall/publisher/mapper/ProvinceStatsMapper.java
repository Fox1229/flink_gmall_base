package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.ProvinceStats;
import org.apache.ibatis.annotations.Select;
import java.util.List;

/**
 * 地区主题mapper接口
 */
public interface ProvinceStatsMapper {

    @Select("select province_name, sum(order_amount) order_amount " +
            "from province_stats " +
            "where toYYYYMMDD(stt) = ${date} " +
            "group by province_id, province_name")
    List<ProvinceStats> getProvinceOrderAmount(Integer date);
}
