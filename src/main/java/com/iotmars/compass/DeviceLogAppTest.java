package com.iotmars.compass;

import com.iotmars.compass.udf.GetDiff;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试在lag开窗函数中，orderby的字段有重复数据，会导致输出结果是错的
 * @author CJ
 * @date: 2023/3/31 19:37
 */
public class DeviceLogAppTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 读取ods层数据
        tableEnv.executeSql("CREATE TABLE ods_items_model (\n" +
                "   `deviceType` STRING,\n" +
                "   `iotId` STRING,\n" +
                "   `requestId` STRING,\n" +
                "   `checkFailedData` STRING,\n" +
                "   `productKey` STRING,\n" +
                "   `gmtCreate` bigint,\n" +
                "   `deviceName` STRING,\n" +
                "   `items` STRING,\n" +
//                        "   `proctime` AS proctime(),\n" +
                "   `ts_ltz` AS TO_TIMESTAMP_LTZ(gmtCreate, 3),\n" +
                "   WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECOND\n" +
                ") WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'topic' = 'items-model',\n" +
                "   'properties.bootstrap.servers' = '192.168.32.44:9092,192.168.32.45:9092,192.168.32.46:9092',\n" +
                "   'properties.group.id' = 'flink-compass-device',\n" +
//                "            'scan.startup.mode' = 'earliest-offset',\n" +
                "            'scan.startup.mode' = 'latest-offset',\n" +
//                "            'scan.startup.mode' = 'group-offsets',\n" +
                "            'format' = 'json'" +
                ")");

        // 处理数据
        tableEnv.createTemporaryFunction("getDiff", GetDiff.class);
        tableEnv.executeSql("CREATE VIEW TmpLagTable AS (" +
                "SELECT deviceType,\n" +
                "       iotId,\n" +
                "       requestId,\n" +
                "       checkFailedData,\n" +
                "       productKey,\n" +
                "       gmtCreate,\n" +
                "       deviceName,\n" +
                "       items                                                               as newData,\n" +
                "       lag(items, 1) over (partition by productKey, iotId order by ts_ltz) as oldData\n" +
                "FROM ods_items_model\n" +
                ")"
        );

        tableEnv.executeSql("CREATE TABLE printTable (\n" +
                "   `deviceType` STRING,\n" +
                "   `iotId` STRING,\n" +
                "   `requestId` STRING,\n" +
                "   `checkFailedData` STRING,\n" +
                "   `productKey` STRING,\n" +
                "   `gmtCreate` bigint,\n" +
                "   `deviceName` STRING,\n" +
                "   `items` STRING,\n" +
                "   `items_old` STRING" +
                ") WITH (\n" +
                "   'connector' = 'print'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO printTable select * from TmpLagTable");

//        env.execute();
    }
}
