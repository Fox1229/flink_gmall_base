package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.VisitorStats;
import java.util.List;

/**
 * 用户访问Service层接口
 */
public interface VisitorStatsService {

    List<VisitorStats> getVisitorStatsByHour(Integer date);

    List<VisitorStats> getVisitorStatsIsNew(Integer date);
}
