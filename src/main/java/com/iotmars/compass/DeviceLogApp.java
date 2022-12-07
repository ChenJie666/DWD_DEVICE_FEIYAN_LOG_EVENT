package com.iotmars.compass;

import com.iotmars.compass.udf.GetDiff;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CJ
 * @date: 2022/10/29 9:17
 */
public class DeviceLogApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
//        env.disableOperatorChaining();

        // 设置checkpoint
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE); // 开启checkpoint，每隔3秒做一次ck，并制定ck的一致性语义
        env.getCheckpointConfig().setCheckpointTimeout(90 * 1000L); // 设置ck超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 设置两次重启的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION // 取消任务时保留外部检查点
        );
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1L), Time.minutes(1L)
        ));

        // 设置StateBackend
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.101.193:8020/flink/checkpoint/");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig config = tableEnv.getConfig();

        config.getConfiguration().setString("parallelism.default", "3");
        config.getConfiguration().setString("table.exec.state.ttl", String.valueOf(24 * 60 * 60 * 1000)); // 如果state状态24小时不变，则回收
        config.getConfiguration().setString("table.exec.source.idle-timeout", String.valueOf(30 * 1000)); // 源在超时时间3000ms内未收到任何元素时，它将被标记为暂时空闲。这允许下游任务推进其水印.

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
                "   WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '30' SECOND\n" +
                ") WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'topic' = 'items-model',\n" +
                "   'properties.bootstrap.servers' = '192.168.101.179:9092,192.168.101.180:9092,192.168.101.181:9092',\n" +
                "   'properties.group.id' = 'flink-compass-device',\n" +
//                "            'scan.startup.mode' = 'earliest-offset',\n" +
//                "            'scan.startup.mode' = 'latest-offset',\n" +
                "            'scan.startup.mode' = 'group-offsets',\n" +
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

        tableEnv.executeSql("CREATE VIEW TmpDiffTable AS (\n" +
                "SELECT deviceType,\n" +
                "       iotId,\n" +
                "       requestId,\n" +
                "       checkFailedData,\n" +
                "       productKey,\n" +
                "       gmtCreate,\n" +
                "       deviceName,\n" +
                "       event_name,\n" +
                "       event_value,\n" +
                "       event_ori_value\n" +
                "FROM TmpLagTable tmp inner join lateral TABLE(getDiff(newData, oldData)) as T(event_name, event_value, event_ori_value) on TRUE" +
                ")"
        );

        // 输出dwd层
        tableEnv.executeSql("CREATE TABLE Kafka193SinkTable (" +
                "   deviceType string,\n" +
                "   iotId string,\n" +
                "   requestId string,\n" +
                "   checkFailedData string,\n" +
                "   productKey string,\n" +
                "   gmtCreate bigint,\n" +
                "   deviceName string,\n" +
                "   eventName string,\n" +
                "   eventValue string,\n" +
                "   eventOriValue string\n" +
                ") WITH (" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_device_feiyan_log_event',\n" +
                "  'properties.bootstrap.servers' = '192.168.101.193:9092,192.168.101.194:9092,192.168.101.195:9092',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO Kafka193SinkTable\n" +
                "    SELECT deviceType,\n" +
                "       iotId,\n" +
                "       requestId,\n" +
                "       checkFailedData,\n" +
                "       productKey,\n" +
                "       gmtCreate,\n" +
                "       deviceName,\n" +
                "       event_name,\n" +
                "       event_value,\n" +
                "       event_ori_value\n" +
                "    FROM TmpDiffTable"
        );


        // 打印到控制台
//        tableEnv.executeSql("CREATE TABLE PrintTable (" +
//                "   deviceType string,\n" +
//                "   iotId string,\n" +
//                "   requestId string,\n" +
//                "   checkFailedData string,\n" +
//                "   productKey string,\n" +
//                "   gmtCreate bigint,\n" +
//                "   deviceName string,\n" +
//                "   eventName string,\n" +
//                "   eventValue string,\n" +
//                "   eventOriValue string\n" +
//                ") WITH (" +
//                "  'connector' = 'print'" +
//                ")");
//        tableEnv.executeSql("INSERT INTO PrintTable " +
//                "    SELECT deviceType,\n" +
//                "       iotId,\n" +
//                "       requestId,\n" +
//                "       checkFailedData,\n" +
//                "       productKey,\n" +
//                "       gmtCreate,\n" +
//                "       deviceName,\n" +
//                "       event_name,\n" +
//                "       event_value,\n" +
//                "       event_ori_value\n" +
//                "    FROM TmpDiffTable"
//        );

        // 落盘到Kafka179中
//        tableEnv.executeSql("CREATE TABLE Kafka179SinkTable (" +
//                "   deviceType string,\n" +
//                "   iotId string,\n" +
//                "   requestId string,\n" +
//                "   checkFailedData string,\n" +
//                "   productKey string,\n" +
//                "   gmtCreate bigint,\n" +
//                "   deviceName string,\n" +
//                "   eventName string,\n" +
//                "   eventValue string,\n" +
//                "   eventOriValue string\n" +
//                ") WITH (" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'dwd_device_feiyan_log_event',\n" +
//                "  'properties.bootstrap.servers' = '192.168.101.179:9092,192.168.101.180:9092,192.168.101.181:9092',\n" +
//                "  'format' = 'json'\n" +
//                ")");
//        tableEnv.executeSql("INSERT INTO Kafka179SinkTable\n" +
//                "    SELECT deviceType,\n" +
//                "       iotId,\n" +
//                "       requestId,\n" +
//                "       checkFailedData,\n" +
//                "       productKey,\n" +
//                "       gmtCreate,\n" +
//                "       deviceName,\n" +
//                "       event_name,\n" +
//                "       event_value,\n" +
//                "       event_ori_value\n" +
//                "    FROM TmpDiffTable"
//        );

        // 落盘到Doris中
//        tableEnv.executeSql("CREATE TABLE MySQLSinkTable (" +
//                "   deviceType string,\n" +
//                "   iotId string,\n" +
//                "   requestId string,\n" +
//                "   checkFailedData string,\n" +
//                "   productKey string,\n" +
//                "   gmtCreate bigint,\n" +
//                "   deviceName string,\n" +
//                "   eventName string,\n" +
//                "   eventValue string,\n" +
//                "   eventOriValue string\n" +
//                ") WITH (" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:mysql://192.168.32.242:9030/compass_device_dev',\n" +
//                "   'table-name' = 'dwd_device_feiyan_log_event'," +
//                "   'username' = 'root'," +
//                "   'password' = '123456'" +
//                ")");
//        tableEnv.executeSql("INSERT INTO MySQLSinkTable\n" +
//                "    SELECT deviceType,\n" +
//                "       iotId,\n" +
//                "       requestId,\n" +
//                "       checkFailedData,\n" +
//                "       productKey,\n" +
//                "       gmtCreate,\n" +
//                "       deviceName,\n" +
//                "       event_name,\n" +
//                "       event_value,\n" +
//                "       event_ori_value\n" +
//                "    FROM TmpDiffTable"
//        );
    }
}