package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 */
public class MyThreadPoolUtils {

    private static volatile ThreadPoolExecutor threadPoolExecutor;

    /**
     * 获取线程池
     */
    public static ThreadPoolExecutor getInstance() {

        if (threadPoolExecutor == null) {
            synchronized (MyThreadPoolUtils.class) {
                if (threadPoolExecutor == null) {
                    System.out.println("-------------- 创建线程池 --------------");
                    threadPoolExecutor = new ThreadPoolExecutor(
                            5,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }

        return threadPoolExecutor;
    }
}
