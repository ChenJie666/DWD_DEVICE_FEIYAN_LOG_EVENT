package com.iotmars.compass.constant;

/**
 * @author CJ
 * @date: 2023/4/14 17:36
 */
public class Constants {

    public static String SOURCE_KAFKA_GROUP_ID = "flink-device_change_log_app_test3"; // 后序需要更换group_id
    public static String SOURCE_KAFKA_BOOTSTRAP_SERVERS = "192.168.32.39:9092,192.168.32.40:9092,192.168.32.41:9092";
    public static String SINK_KAFKA_BOOTSTRAP_SERVERS = "192.168.32.53:9092,192.168.32.54:9092,192.168.32.55:9092";
    public static String SINK_KAFKA_TOPIC = "dwd_device_feiyan_log_late_event";
    public static String SINK_KAFKA_LATE_TOPIC = "dwd_device_feiyan_log_late_event";

//    public static String CHECKPOINT_STORAGE = "hdfs://192.168.101.193:8020/flink/checkpoint/msas_prod/msas_device_exceptions";
    public static String CHECKPOINT_STORAGE = "hdfs:///flink/checkpoint/device/dwd_device_feiyan_log_event/prod";
//    public static int PARALLELISM = 1;
}
