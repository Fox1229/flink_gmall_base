package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.ProvinceStats;
import java.util.List;

/**
 * 省份指标统计
 */
public interface ProvinceStatsService {

    List<ProvinceStats> getProvinceOrderAmount(Integer date);
}
