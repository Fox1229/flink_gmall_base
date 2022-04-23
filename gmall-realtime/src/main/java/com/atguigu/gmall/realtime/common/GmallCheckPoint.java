package com.atguigu.gmall.realtime.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;
import static com.atguigu.gmall.realtime.common.GmallConfig.HADOOP_USER_NAME;

/**
 * 检查点配置
 */
public class GmallCheckPoint {

    private static final GmallCheckPoint GMALLCHECKPOINT = new GmallCheckPoint();

    private GmallCheckPoint() {
    }

    public static GmallCheckPoint getInstance() {
        return GMALLCHECKPOINT;
    }

    /**
     * 设置检查点
     * @param env flink实时运行环境
     */
    public void setCheckPoint(StreamExecutionEnvironment env) {

        // 开启检查点，设置精准一次消费
        env.enableCheckpointing(CHECKPOINT_PERIOD, CheckpointingMode.EXACTLY_ONCE);
        // 是指检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        // 取消job是否保存检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CHECKPOINT_MIN_BETWEEN);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend(HDFS_CHECKPOINT_PATH));
        // 设置操作用户
        System.setProperty(HADOOP_USER_KEY, HADOOP_USER_NAME);
        // 设置失败重试周期
        env.setRestartStrategy(RestartStrategies.failureRateRestart(FAIL_RATE, Time.days(30), Time.seconds(3)));
    }
}
