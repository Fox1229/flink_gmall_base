package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.ProductStats;
import java.math.BigDecimal;
import java.util.List;

/**
 * 商品主题统计Service层接口
 */
public interface ProductStatsService {

    /**
     * spu销售额和下单数
     */
    List<ProductStats> getProductStatsBySPU(Integer date, Integer limit);

    /**
     * 品类销售额
     */
    List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit);

    /**
     * 商品销售额TopN
     */
    List<ProductStats> getProductStatsByTm(Integer date, Integer limit);

    /**
     * 获取某一天的订单总额
     */
    BigDecimal getOrderAmount(Integer date);
}
