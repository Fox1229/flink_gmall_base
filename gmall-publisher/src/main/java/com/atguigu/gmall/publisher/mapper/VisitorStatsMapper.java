package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 访客主题mapper层接口
 */
public interface VisitorStatsMapper {

    /**
     * 每小时实时统计
     */
    @Select("SELECT toHour(stt) hr, sum(uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(if(is_new = '1', visitor_stats.uv_ct, 0)) new_uv " +
            "FROM visitor_stats " +
            "WHERE toYYYYMMDD(stt) = 20220503 " +
            "GROUP BY hr")
    List<VisitorStats> selectVisitorStatsByHour(Integer date);

    /**
     * 新老用户访问信息
     */
    @Select("SELECT is_new, sum(uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(sv_ct) sv_ct, sum(uj_ct) uj_ct, sum(dur_sum) AS dur_Sum " +
            "FROM visitor_stats " +
            "WHERE toYYYYMMDD(stt) = #{date} " +
            "GROUP BY is_new")
    List<VisitorStats> selectVisitorStatsIsNew(Integer date);
}
