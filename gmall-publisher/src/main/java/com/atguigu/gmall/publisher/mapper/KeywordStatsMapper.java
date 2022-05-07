package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import java.util.List;

public interface KeywordStatsMapper {

    /**
     * 查询热搜关键的
     */
    @Select("select keyword, sum(keyword_stats.ct * multiIf( " +
            "source = 'SEARCH', 10," +
            "source = 'ORDER', 5," +
            "source = 'CART', 2," +
            "source = 'CLICK', 1, 0 " +
            ")) ct " +
            "from keyword_stats " +
            "where toYYYYMMDD(stt) = ${date} " +
            "group by keyword " +
            "order by ct desc " +
            "limit ${limit}")
    List<KeywordStats> selectKeywordStatsByCt(@Param(value = "date") Integer date, @Param(value = "limit") Integer limit);
}
