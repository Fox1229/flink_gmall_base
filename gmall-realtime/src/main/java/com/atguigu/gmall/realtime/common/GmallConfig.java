package com.atguigu.gmall.realtime.common;


/**
 * 配置信息类
 */
public class GmallConfig {

    // TODO system
    public static final String SINK_TABLE_KEY = "sink_table";
    public static final Long ASYNC_REQUEST_TIMEOUT = 60L;

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
    public static final String KAFKA_DEFAULT_TOPIC = "gmall_default_topic";
    public static final String DWD_PAGE_LOG = "f_dwd_page_log";
    public static final String DWD_START_LOG = "f_dwd_start_log";
    public static final String DWD_DISPLAY_LOG = "f_dwd_display_log";
    public static final String DWD_ORDER_INFO = "f_dwd_order_info";
    public static final String DWD_ORDER_DETAIL = "f_dwd_order_detail";
    public static final String DWD_PAYMENT_INFO = "f_dwd_payment_info";
    public static final String DWD_FAVOR_INFO = "f_dwd_favor_info";
    public static final String DWD_CART_INFO = "f_dwd_cart_info";
    public static final String DWD_COMMENT_INFO = "f_dwd_comment_info";
    public static final String DWM_ORDER_REFUND_INFO = "f_dwd_order_refund_info";
    // dwm层
    public static final String DWM_UNIQUE_VISITOR = "f_dwm_unique_visitor";
    public static final String DWM_UNIQUE_VISITOR_GROUP_ID = "f_dwm_unique_visitor_group_id";
    public static final String DWM_USER_JUMP_DETAIL = "f_dwm_user_jump_detail";
    public static final String DWM_USER_JUMP_DETAIL_GROUP_ID = "f_dwm_user_jump_detail_group_id";
    public static final String DWM_ORDER_WIDE = "f_dwm_order_wide";
    public static final String DWM_ORDER_WIDE_GROUP_ID = "f_dwm_order_wide_group_id";
    public static final String DWM_PAYMENT_WIDE = "f_dwm_payment_wide";
    public static final String DWM_PAYMENT_WIDE_GROUP_ID = "f_dwm_payment_wide_group_id";
    // dws层
    public static final String DWS_VISITOR_STATS_GROUP_ID = "f_dws_visitor_stats_group_id";
    public static final String DWS_PRODUCT_STATS_GROUP_ID = "f_dws_product_stats_group_id";
    public static final String DWS_PROVINCE_STATS_GROUP_ID = "f_dws_province_stats_group_id";
    public static final String DWS_KEYWORD_STATS_GROUP_ID = "f_dws_keyword_stats_group_id";

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
    public static final String PHOENIX_OP_UPDATE = "update";
    // 对Phoenix的历史数据同步
    public static final String PHOENIX_OP_BOOTSTRAP_INSERT = "bootstrap-insert";
    // table
    public static final String PHOENIX_DIM_USER_INFO = "dim_user_info";
    public static final String PHOENIX_DIM_BASE_PROVINCE = "dim_base_province";
    public static final String PHOENIX_DIM_SKU_INFO = "dim_sku_info";
    public static final String PHOENIX_DIM_SPU_INFO = "dim_spu_info";
    public static final String PHOENIX_DIM_BASE_CATEGORY1 = "dim_base_category1";
    public static final String PHOENIX_DIM_BASE_CATEGORY2 = "dim_base_category2";
    public static final String PHOENIX_DIM_BASE_CATEGORY3 = "dim_base_category3";
    public static final String PHOENIX_DIM_BASE_TRADEMARK = "dim_base_trademark";

    // TODO tableProcess
    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    // TODO Redis
    public static final String REDIS_HOST = "hadoop102";
    public static final Integer REDIS_PORT = 6379;
    public static final Integer REDIS_TIMEOUT = 10000;
    public static final Integer REDIS_KEY_EXPIRE = 60 * 60 * 24;
}
