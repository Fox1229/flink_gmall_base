package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 纬度关联接口
 */
public interface DimAsyncInterface<T> {


    /**
     * 获取主键
     */
    String getPrimaryKey(T obj);

    /**
     * 主流与纬度数据关联
     * @param obj 主流
     * @param dimInfo 纬度
     */
    void join(T obj, JSONObject dimInfo);
}
