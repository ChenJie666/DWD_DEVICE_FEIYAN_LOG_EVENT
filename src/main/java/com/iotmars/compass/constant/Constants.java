package com.iotmars.compass.constant;

/**
 * @author CJ
 * @date: 2023/4/14 17:36
 */
public class Constants {

    public static String SOURCE_KAFKA_GROUP_ID = "flink-device_change_log_app_test3";
    public static String SOURCE_KAFKA_BOOTSTRAP_SERVERS = "192.168.101.179:9092,192.168.101.180:9092,192.168.101.181:9092";
    public static String SINK_KAFKA_BOOTSTRAP_SERVERS = "192.168.101.193:9092,192.168.101.194:9092,192.168.101.195:9092";
    public static String SINK_KAFKA_TOPIC = "dwd_device_feiyan_log_late_event";
    public static String SINK_KAFKA_LATE_TOPIC = "dwd_device_feiyan_log_late_event";

//    public static String CHECKPOINT_STORAGE = "hdfs://192.168.101.193:8020/flink/checkpoint/msas_prod/msas_device_exceptions";
    public static String CHECKPOINT_STORAGE = "hdfs:///flink/checkpoint/dwd_device_feiyan_log_event/prod";
//    public static int PARALLELISM = 1;
}
