package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.ProvinceStats;
import com.atguigu.gmall.publisher.mapper.ProvinceStatsMapper;
import com.atguigu.gmall.publisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    private ProvinceStatsMapper provinceStatsMapper;

    /**
     * 获取当天每个省份的交易额
     */
    @Override
    public List<ProvinceStats> getProvinceOrderAmount(Integer date) {
        return provinceStatsMapper.getProvinceOrderAmount(date);
    }
}
