package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtils;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * 订单宽表，发送异步请求
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncInterface<T> {

    private ExecutorService threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtils.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.submit(
                new Runnable() {
                    @Override
                    public void run() {

                        long start = System.currentTimeMillis();

                        // 获取纬度关联的主键
                        String pk = getPrimaryKey(obj);

                        // 查询纬度信息
                        JSONObject dimInfo = DimUtils.getDimInfo(tableName, pk);

                        // 将纬度信息应用到主流
                        if (dimInfo != null) {
                            join(obj, dimInfo);
                        }

                        // 将结果回调
                        resultFuture.complete(Collections.singleton(obj));

                        long end = System.currentTimeMillis();
                        System.out.println("纬度关联耗时：" + (end - start) + " ms");
                    }
                }
        );
    }
}
