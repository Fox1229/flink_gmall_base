package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.ProductStats;
import com.atguigu.gmall.publisher.mapper.ProductStatsMapper;
import com.atguigu.gmall.publisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.util.List;

/**
 * 商品主题统计Service层接口实现类
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    /**
     * spu销售额和下单数
     */
    @Override
    public List<ProductStats> getProductStatsBySPU(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsBySPU(date, limit);
    }

    /**
     * 品类销售额
     */
    @Override
    public List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCategory3(date, limit);
    }

    /**
     * 品牌销售额
     */
    @Override
    public List<ProductStats> getProductStatsByTm(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByTm(date, limit);
    }

    /**
     * 获取某天订单总额
     */
    @Override
    public BigDecimal getOrderAmount(Integer date) {
        return productStatsMapper.selectOrderAmount(date);
    }
}
