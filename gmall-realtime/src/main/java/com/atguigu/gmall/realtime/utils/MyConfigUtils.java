package com.atguigu.gmall.realtime.utils;


/**
 * 配置信息类
 */
public class MyConfigUtils {

    // TODO flink
    public static final Integer PARALLELISM_NUM = 4;

    // TODO hdfs
    public static final String HADOOP_USER_KEY = "HADOOP_USER_NAME";
    public static final String HADOOP_USER_NAME = "atguigu";
    public static final String HDFS_CHECKPOINT_PATH = "hdfs://hadoop102:8020/gmall/ck";

    // TODO kafka
    public static final String KAFKA_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    // ods层
    public static final String ODS_BASE_LOG = "f_ods_base_log";
    public static final String ODS_BASE_LOG_GROUP_ID = "f_ods_base_log_gid";
    public static final String ODS_BASE_DB = "f_ods_base_db_m";
    public static final String ODS_BASE_DB_GROUP_ID = "f_ods_base_db_m_gid";
    // dwd层
    public static final String DWD_PAGE_LOG = "f_dwd_page_log";
    public static final String DWD_START_LOG = "f_dwd_start_log";
    public static final String DWD_DISPLAY_LOG = "f_dwd_display_log";

    // TODO MySQL
    public static final String MYSQL_HOST_NAME = "hadoop102";
    public static final Integer MYSQL_PORT = 3306;
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String GMALL_CONFIG_DBS = "flink_gmall_config";
    public static final String GMALL_CONFIG_TABLES = "flink_gmall_config.table_process";
}
