package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.VisitorStats;
import com.atguigu.gmall.publisher.mapper.VisitorStatsMapper;
import com.atguigu.gmall.publisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    private VisitorStatsMapper visitorStatsMapper;

    /**
     * 访客每小时统计信息
     */
    @Override
    public List<VisitorStats> getVisitorStatsByHour(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }

    /**
     * 新老用户访问信息
     */
    @Override
    public List<VisitorStats> getVisitorStatsIsNew(Integer date) {
        return visitorStatsMapper.selectVisitorStatsIsNew(date);
    }
}
