package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import java.util.List;

/**
 * 关键词service层接口
 */
public interface KeywordStatsService {

    List<KeywordStats> getKeywordStatsByCt(Integer date, Integer limit);
}
