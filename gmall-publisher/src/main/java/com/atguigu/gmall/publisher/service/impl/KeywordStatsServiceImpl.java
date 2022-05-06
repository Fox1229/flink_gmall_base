package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import com.atguigu.gmall.publisher.mapper.KeywordStatsMapper;
import com.atguigu.gmall.publisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    private KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStatsByCt(Integer date, Integer limit) {
        return keywordStatsMapper.selectKeywordStatsByCt(date, limit);
    }
}
