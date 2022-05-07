package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品主题mapper层接口
 */
public interface ProductStatsMapper {

    /**
     * 获取每个spu的销售额和下单数
     */
    @Select("select spu_id, spu_name, sum(order_amount) order_amount, sum(order_ct) order_ct " +
            "from product_stats " +
            "where toYYYYMMDD(stt) = ${date} " +
            "group by spu_id, spu_name " +
            "having order_amount > 0 " +
            "order by order_amount desc " +
            "limit ${limit}")
    List<ProductStats> selectProductStatsBySPU(@Param(value = "date") Integer date, @Param(value = "limit")Integer limit);

    /**
     * 获取每个品类的销售额
     */
    @Select("select category3_id, category3_name, sum(order_amount) order_amount " +
            "from product_stats " +
            "where toYYYYMMDD(stt) = ${date} " +
            "group by category3_id, category3_name " +
            "having order_amount > 0 " +
            "order by order_amount desc " +
            "limit ${limit}")
    List<ProductStats> selectProductStatsByCategory3(@Param(value = "date") Integer date, @Param(value = "limit")Integer limit);

    /**
     * TopN: 获取每个品牌的销售额
     */
    @Select("select tm_id, tm_name, sum(order_amount) order_amount " +
            "from product_stats " +
            "where toYYYYMMDD(stt) = ${date} " +
            "group by tm_id, tm_name " +
            "having order_amount > 0 " +
            "order by order_amount desc " +
            "limit ${limit}")
    List<ProductStats> selectProductStatsByTm(@Param(value = "date") Integer date, @Param(value = "limit")Integer limit);

    /**
     * 获取某一天交易总额
     */
    @Select("select sum(order_amount) from product_stats where toYYYYMMDD(stt) = ${date}")
    BigDecimal selectOrderAmount(Integer date);
}
