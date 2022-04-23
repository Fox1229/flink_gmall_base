package com.atguigu.gmall.realtime.common;


/**
 * 配置信息类
 */
public class GmallConfig {

    // TODO system
    public static final String SINK_TABLE_KEY = "sink_table";

    // TODO flink
    public static final Integer PARALLELISM_NUM = 4;
    // CheckPoint
    public static final Long CHECKPOINT_PERIOD = 5 * 1000L;
    public static final Long CHECKPOINT_TIMEOUT = 60 * 1000L;
    public static final Long CHECKPOINT_MIN_BETWEEN = 60 * 1000L;
    public static final Integer FAIL_RATE = 3;

    // TODO hdfs
    public static final String HADOOP_USER_KEY = "HADOOP_USER_NAME";
    public static final String HADOOP_USER_NAME = "atguigu";
    public static final String HDFS_CHECKPOINT_PATH = "hdfs://hadoop102:8020/gmall/ck";

    // TODO kafka
    public static final String KAFKA_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String PRODUCER_TRANSACTION_TIMEOUT = 15 * 60 * 1000 + "";
    // ods层
    public static final String ODS_BASE_LOG = "f_ods_base_log";
    public static final String ODS_BASE_LOG_GROUP_ID = "f_ods_base_log_gid";
    public static final String ODS_BASE_DB = "f_ods_base_db_m";
    public static final String ODS_BASE_DB_GROUP_ID = "f_ods_base_db_m_gid";
    // dwd层
    public static final String DWD_PAGE_LOG = "f_dwd_page_log";
    public static final String DWD_PAGE_LOG_GROUP_ID = "f_dwd_page_log_group_id";
    public static final String DWD_START_LOG = "f_dwd_start_log";
    public static final String DWD_START_LOG_GROUP_ID = "f_dwd_start_log_group_id";
    public static final String DWD_DISPLAY_LOG = "f_dwd_display_log";
    public static final String DWD_DISPLAY_LOG_GROUP_ID = "f_dwd_display_log_group_id";
    public static final String KAFKA_DEFAULT_TOPIC = "gmall_default_topic";
    // dwm层
    public static final String DWM_UNIQUE_VISITOR = "f_dwm_unique_visitor";
    public static final String DWM_UNIQUE_VISITOR_GROUP_ID = "f_dwm_unique_visitor_group_id";

    // TODO MySQL
    public static final String MYSQL_HOST_NAME = "hadoop102";
    public static final Integer MYSQL_PORT = 3306;
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String GMALL_CONFIG_DBS = "flink_gmall_config";
    public static final String GMALL_CONFIG_TABLES = "flink_gmall_config.table_process";

    // TODO phoenix
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    // ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
    // ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    // 对Phoenix的insert操作
    public static final String PHOENIX_OP_INSERT = "insert";
    // 对Phoenix的历史数据同步
    public static final String PHOENIX_OP_BOOTSTRAP_INSERT = "bootstrap-insert";

    // TODO tableProcess
    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
}
