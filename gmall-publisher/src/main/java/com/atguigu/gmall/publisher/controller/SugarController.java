package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import com.atguigu.gmall.publisher.beans.ProductStats;
import com.atguigu.gmall.publisher.beans.ProvinceStats;
import com.atguigu.gmall.publisher.beans.VisitorStats;
import com.atguigu.gmall.publisher.service.KeywordStatsService;
import com.atguigu.gmall.publisher.service.ProductStatsService;
import com.atguigu.gmall.publisher.service.ProvinceStatsService;
import com.atguigu.gmall.publisher.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 大屏展示控制类
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private ProductStatsService productStatsService;
    @Autowired
    private ProvinceStatsService provinceStatsService;
    @Autowired
    private KeywordStatsService keywordStatsService;
    @Autowired
    private VisitorStatsService visitorStatsService;

    @RequestMapping("/keyword")
    public String getKeywordCnt(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 0) {
            date = now();
        }

        List<KeywordStats> keywordStatsByCt = keywordStatsService.getKeywordStatsByCt(date, limit);
        StringBuilder sb = new StringBuilder("{\"status\": 0,\"data\": [");
        for (int i = 0; i < keywordStatsByCt.size(); i++) {
            KeywordStats keywordStats = keywordStatsByCt.get(i);
            sb
                    .append("{\"name\": \"").append(keywordStats.getKeyword())
                    .append("\",\"value\": ").append(keywordStats.getCt()).append("}");
            if (i != keywordStatsByCt.size() - 1) {
                sb.append(",");
            }
        }

        sb.append("]}");
        return sb.toString();
    }

    @RequestMapping("/hour")
    public String getVisitorStatsByHour(@RequestParam(value = "date", defaultValue = "0") Integer date) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        // 每小时uv数，pv数，新用户数
        List<VisitorStats> visitorStatsList = visitorStatsService.getVisitorStatsByHour(date);

        // 定义数组，用于存放一天24个小时的访问情况
        VisitorStats[] visitorArr = new VisitorStats[24];
        for (VisitorStats visitorStats : visitorStatsList) {
            visitorArr[visitorStats.getHr()] = visitorStats;
        }

        // 对数据进行遍历，获取一天24小时对应的统计数据
        ArrayList<String> hrList = new ArrayList<>();
        ArrayList<Long> uvList = new ArrayList<>();
        ArrayList<Long> pvList = new ArrayList<>();
        ArrayList<Long> newUvList = new ArrayList<>();

        for (int i = 0; i < visitorArr.length; i++) {
            VisitorStats tmp = visitorArr[i];
            if (tmp != null) {
                // 当前小时有访客数据
                uvList.add(tmp.getUv_ct());
                pvList.add(tmp.getPv_ct());
                newUvList.add(tmp.getNew_uv());
            } else {
                // 当前小时没有访客数据
                uvList.add(0L);
                pvList.add(0L);
                newUvList.add(0L);
            }
            hrList.add(String.format("%02d", i));
        }

        return "{\"status\":0,\"data\":{\"" +
                "categories\":[\"" + StringUtils.join(hrList, "\",\"") + "\"],\"series\":[{\"name\":\"" +
                "uv\",\"data\":[" + StringUtils.join(uvList, ",") + "]},{\"name\":\"" +
                "pv\",\"data\":[" + StringUtils.join(pvList, ",") + "]},{\"name\":\"" +
                "新用户\",\"data\":[" + StringUtils.join(newUvList, ",") + "]}]}}";
    }

    @RequestMapping("/isNew")
    public String getVisitorStatsIsNew(@RequestParam(value = "date", defaultValue = "0") Integer date) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        // 获取新老用户访问信息
        List<VisitorStats> visitorStatsList = visitorStatsService.getVisitorStatsIsNew(date);

        VisitorStats newVisitor = new VisitorStats();
        VisitorStats oldVisitor = new VisitorStats();
        for (VisitorStats tmp : visitorStatsList) {
            if ("1".equals(tmp.getIs_new())) {
                newVisitor = tmp;
            } else {
                oldVisitor = tmp;
            }
        }

        return "{\"status\": 0,\"data\": {\"total\": 5,\"columns\": [{\"name\": \"" +
                "类别\",\"id\": \"type\"},{\"name\": \"" +
                "新用户\",\"id\": \"new_user\"},{\"name\": \"" +
                "老用户\",\"id\": \"old_user\"}],\"" +
                "rows\": [{\"type\": \"" +
                "用户数（人）\",\"new_user\": " + newVisitor.getUv_ct() + ",\"old_user\": " + oldVisitor.getUv_ct() + "},{\"type\": \"" +
                "总访问页面（次）\",\"new_user\": " + newVisitor.getPv_ct() + ",\"old_user\": " + oldVisitor.getPv_ct() + "},{\"type\": \"" +
                "跳出率（%）\",\"new_user\": " + newVisitor.getUjRate() + ",\"old_user\": " + oldVisitor.getUjRate() + "},{\"type\": \"" +
                "平均在线时长（秒）\",\"new_user\": " + newVisitor.getDurPerSv() + ",\"old_user\": " + oldVisitor.getDurPerSv() + "},{\"type\": \"" +
                "平均访问页面数（人次）\",  \"new_user\": " + newVisitor.getPvPerSv() + ",  \"old_user\": " + oldVisitor.getPvPerSv() + "}]}}";
    }

    @RequestMapping("/map")
    public String getProvinceOrderAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        // 查询数据
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceOrderAmount(date);

        StringBuilder sb = new StringBuilder("{\"status\": 0,\"data\":{\"mapData\":[");
        if (provinceStatsList.size() > 0) {
            for (int i = 0; i < provinceStatsList.size(); i++) {
                ProvinceStats provinceStats = provinceStatsList.get(i);
                sb
                        .append("{\"name\":\"")
                        .append(provinceStats.getProvince_name())
                        .append("\",\"value\":")
                        .append(provinceStats.getOrder_amount())
                        .append("}");

                if (i != provinceStatsList.size() - 1) {
                    sb.append(",");
                }
            }
        }

        sb.append("]}}");
        return sb.toString();
    }

    /**
     * 获取spu销售额和下单数
     */
    @RequestMapping("/spu")
    public String getProductStatsBySPU(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        // 获取spu销售额和下单数
        List<ProductStats> productStatsBySPU = productStatsService.getProductStatsBySPU(date, limit);

        StringBuilder sb = new StringBuilder("{\"status\": 0,\"data\": {\"columns\": [{\"name\": \"商品spu名称\",\"id\": \"spu_name\"}," +
                "{\"name\": \"交易额\",\"id\": \"order_amount\"},{\"name\": \"订单数\",\"id\": \"order_ct\"}],\"rows\": [");
        for (int i = 0; i < productStatsBySPU.size(); i++) {
            ProductStats productStats = productStatsBySPU.get(i);
            sb.append("{\"spu_name\": \"").append(productStats.getSpu_name())
                    .append("\",\"order_amount\": ").append(productStats.getOrder_amount())
                    .append(",\"order_ct\": ").append(productStats.getOrder_ct()).append("}");

            if (i != productStatsBySPU.size() - 1) {
                sb.append(",");
            }
        }

        sb.append("]}}");

        return sb.toString();
    }

    /**
     * 各品类销售额
     */
    @RequestMapping("/category3")
    public String getProductStatsByCategory3(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        // 获取品类销售额
        List<ProductStats> productStatsByCategory3 = productStatsService.getProductStatsByCategory3(date, limit);

        StringBuilder sb = new StringBuilder("{\"status\": 0,\"data\": [");
        for (int i = 0; i < productStatsByCategory3.size(); i++) {
            ProductStats productStats = productStatsByCategory3.get(i);
            sb
                    .append("{\"name\":\"").append(productStats.getCategory3_name())
                    .append("\",\"value\":").append(productStats.getOrder_amount()).append("}");

            if (i != productStatsByCategory3.size() - 1) {
                sb.append(",");
            }
        }

        sb.append("]}");
        return sb.toString();
    }

    /**
     * 各品牌销售额TopN
     */
    @RequestMapping("/tm")
    public String getProductStatsByTm(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        // 获取商品品牌交易额
        List<ProductStats> productStatsList = productStatsService.getProductStatsByTm(date, limit);

        // 定义新的集合，保存品牌名和品牌交易额
        ArrayList<String> tmList = new ArrayList<>(productStatsList.size());
        ArrayList<BigDecimal> amountList = new ArrayList<>(productStatsList.size());
        for (ProductStats tmp : productStatsList) {
            tmList.add(tmp.getTm_name());
            amountList.add(tmp.getOrder_amount());
        }

        return "{\"status\": 0,\"data\": {\"" +
                "categories\": [\" " + StringUtils.join(tmList, "\",\"") + "\"],\"" +
                "series\": [{\"name\": \"商品品牌\",\"data\": [" + StringUtils.join(amountList, ",") + "]}]}}";
    }

    /**
     * 获取某天销售总额
     */
    @RequestMapping("/gmv")
    public String getOrderAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {

        // 若用户没有输入日期，默认查询当天日期
        if (date == 0) {
            date = now();
        }

        BigDecimal orderAmount = productStatsService.getOrderAmount(date);

        return "{\"status\": 0,\"data\": " + orderAmount + "}";
    }

    private Integer now() {
        String now = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(now);
    }
}
