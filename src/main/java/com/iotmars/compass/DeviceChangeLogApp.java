package com.iotmars.compass;

import com.alibaba.fastjson.JSONObject;
import com.iotmars.compass.constant.Constants;
import com.iotmars.compass.entity.ItemsModelEventDTO;
import com.iotmars.compass.util.JSONKeyValueDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsonschema.JsonSerializableSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleSerializers;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * @author CJ
 * @date: 2023/3/31 23:13
 */
public class DeviceChangeLogApp {
    private static final Logger logger = LoggerFactory.getLogger(DeviceChangeLogApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
//        env.disableOperatorChaining();

        // 设置checkpoint
        env.enableCheckpointing(5 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE); // 开启checkpoint，并制定ck的一致性语义
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L); // 设置ck超时时间
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
        env.getCheckpointConfig().setCheckpointStorage(Constants.CHECKPOINT_STORAGE);

        // 读取Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constants.SOURCE_KAFKA_BOOTSTRAP_SERVERS);
//        properties.setProperty("bootstrap.servers", "192.168.32.242:9092");
        properties.setProperty("group.id", Constants.SOURCE_KAFKA_GROUP_ID);
        FlinkKafkaConsumerBase<ObjectNode> objectNodeFlinkKafkaConsumerBase = new FlinkKafkaConsumer<>("items-model", new JSONKeyValueDeserializationSchema(true), properties);

        // 水位线容许延迟时间
        int waterMarkSeconds = 30;

        // TODO 根据业务修改offset位置
//        objectNodeFlinkKafkaConsumerBase.setStartFromEarliest();
//        objectNodeFlinkKafkaConsumerBase.setStartFromLatest();
//        objectNodeFlinkKafkaConsumerBase.setStartFromTimestamp(1680278400000L); // 2023-04-01 11:00:00
        objectNodeFlinkKafkaConsumerBase.setStartFromGroupOffsets();
        DataStreamSource<ObjectNode> objectNodeDataStreamSource = env.addSource(objectNodeFlinkKafkaConsumerBase);

        // TODO 迟到数据到侧输出流
        OutputTag<ItemsModelEventDTO> lateOutputTag = new OutputTag<ItemsModelEventDTO>("LateOutputTag") {
        };

        SingleOutputStreamOperator<ItemsModelEventDTO> itemsModelEventDataStream = objectNodeDataStreamSource
                .process(new ProcessFunction<ObjectNode, ItemsModelEventDTO>() {
                             @Override
                             public void processElement(ObjectNode objectNode, Context ctx, Collector<ItemsModelEventDTO> out) throws Exception {
                                 String deviceType;
                                 String iotId;
                                 String requestId;
                                 String checkFailedData;
                                 String productKey;
                                 String deviceName;
                                 Iterator<Map.Entry<String, JsonNode>> fields;
                                 try {
                                     JsonNode logJsonNode = objectNode.get("value");
                                     deviceType = logJsonNode.get("deviceType").textValue();
                                     iotId = logJsonNode.get("iotId").textValue();
                                     requestId = logJsonNode.get("requestId").textValue();
                                     checkFailedData = logJsonNode.get("checkFailedData").textValue();
                                     productKey = logJsonNode.get("productKey").textValue();
                                     deviceName = logJsonNode.get("deviceName").textValue();
                                     fields = logJsonNode.get("items").fields();
                                 } catch (Exception e) {
                                     logger.error("无items元素，非设备状态日志");
                                     return;
                                 }

//                                 Iterator<Map.Entry<String, JsonNode>> fields = itemsJsonObject.fields();

                                 // 用于存储联动属性
                                 HashMap<String, Long> linkEntries = new HashMap<>();
                                 ArrayList<String> linkValues = new ArrayList<>();
                                 ArrayList<String> linkOriValues = new ArrayList<>();

                                 // 解析items中元素并每个元素创建一个对象
                                 while (fields.hasNext()) {
                                     Map.Entry<String, JsonNode> next = fields.next();
                                     String eventName = next.getKey();
                                     JsonNode value = next.getValue();
                                     String eventValueCode = value.get("value").asText();
                                     Long eventTimestamp = value.get("time").longValue();

                                     // 有些属性需要联动，所以eventValue存储时带上eventName，对于联动的属性，eventValue中都需要带上
                                     if ("CookbookID".equals(eventName) || "MultiStageName".equals(eventName) || "StOvMode".equals(eventName) || "LStOvMode".equals(eventName) || "RStOvState".equals(eventName)) {
                                         linkEntries.put(eventName, eventTimestamp);
                                         linkValues.add(eventName + ":" + eventValueCode);
                                         linkOriValues.add(eventName + ":");
                                     } else {
                                         String eventValue = eventName + ":" + eventValueCode;
                                         String eventOriValue = eventName + ":";
                                         ItemsModelEventDTO itemsModelEventDTO = new ItemsModelEventDTO(deviceType, iotId, requestId, checkFailedData, productKey, eventTimestamp, deviceName, eventName, eventValue, eventOriValue);
                                         out.collect(itemsModelEventDTO);
//                                         System.out.println("输出单独属性：" + itemsModelEventDTO);
                                     }
                                 }

                                 // 输出联动属性
                                 String linkValuesString = String.join(",", linkValues);
                                 String linkOriValuesString = String.join(",", linkOriValues);
                                 linkEntries.forEach((eventName, event_timestamp) -> {
                                     ItemsModelEventDTO itemsModelEventDTO = new ItemsModelEventDTO(deviceType, iotId, requestId, checkFailedData, productKey, event_timestamp, deviceName, eventName, linkValuesString, linkOriValuesString);
                                     out.collect(itemsModelEventDTO);
//                                     System.out.println("输出联动属性：" + itemsModelEventDTO);
                                 });

                             }
                         }
                );

        SingleOutputStreamOperator<ItemsModelEventDTO> itemsModelEventDTOSingleOutputStreamOperator = itemsModelEventDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ItemsModelEventDTO>forBoundedOutOfOrderness(Duration.ofSeconds(waterMarkSeconds)).withTimestampAssigner(new SerializableTimestampAssigner<ItemsModelEventDTO>() {
                    @Override
                    public long extractTimestamp(ItemsModelEventDTO itemsModelEventDTO, long recordTimestamp) {
                        return itemsModelEventDTO.getGmtCreate();
                    }
                }));


        SingleOutputStreamOperator<ItemsModelEventDTO> resultDataStream = itemsModelEventDTOSingleOutputStreamOperator
                // 通过比较获取操作数据
                .keyBy(itemsModelEventDTO -> itemsModelEventDTO.getProductKey() + "_" + itemsModelEventDTO.getDeviceName() + "_" + itemsModelEventDTO.getEventName())
                .process(new KeyedProcessFunction<String, ItemsModelEventDTO, ItemsModelEventDTO>() {
                             private ListState<ItemsModelEventDTO> listState;
                             private ValueState<Boolean> isFirstFlagState;

                             @Override
                             public void open(Configuration parameters) throws Exception {
                                 super.open(parameters);
                                 listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemsModelEventDTO>("compare-list", ItemsModelEventDTO.class));
                                 isFirstFlagState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-first-flag", Boolean.class));
//                                 isFirstFlagState.update(true);
                             }

                             @Override
                             public void processElement(ItemsModelEventDTO itemsModelEventDTO, Context ctx, Collector<ItemsModelEventDTO> out) throws Exception {
                                 long currentWatermark = ctx.timerService().currentWatermark();

                                 // 每来一条数据，插入到状态后端中；如果数据的事件时间超过延迟时间，则将数据输出到侧输出流
                                 long eventTime = itemsModelEventDTO.getGmtCreate();
//                                 logger.warn("验证" + itemsModelEventDTO.getEventName() + "数据到达processElement时，水位线是否已经被该数据更新:" + currentWatermark);
//                                 logger.warn(itemsModelEventDTO.getEventName() + "数据到达processElement时该数据的eventTime:" + eventTime);

                                 // 如果不是第一条数据且事件时间小于水位线时间的为迟到事件
                                 if (eventTime <= currentWatermark && currentWatermark != Long.MIN_VALUE) {
                                     ctx.output(lateOutputTag, itemsModelEventDTO);
                                 } else {
                                     // 每来一条数据定义一个当前时间+1毫秒的定时器，用于排序和输出结果
                                     listState.add(itemsModelEventDTO);
                                     ctx.timerService().registerEventTimeTimer(eventTime + 1);
                                 }

//                                 // 注意，如果状态中没有记录，那么这条记录直接输出作为初始状态。
//                                 if (!listState.get().iterator().hasNext()) {
//                                     listState.add(itemsModelEventDTO);
//                                     out.collect(itemsModelEventDTO);
//                                 } else {
//                                     listState.add(itemsModelEventDTO);
//                                     // 定义一个当前时间+1毫秒的定时器，用于排序和输出结果
//                                     ctx.timerService().registerEventTimeTimer(eventTime + 1);
//                                 }
                             }

                             @Override
                             public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemsModelEventDTO> out) throws Exception {
//                                 logger.warn("onTimer timestamp:" + timestamp);

                                 Iterable<ItemsModelEventDTO> itemsModelEventIterable = listState.get();

                                 // 获取超过水位线的记录，进行排序
                                 TreeSet<ItemsModelEventDTO> outWatermarkItemsModelEventSet = new TreeSet<>();
                                 ArrayList<ItemsModelEventDTO> inWatermarkItemsModelEventList = new ArrayList<>();
                                 itemsModelEventIterable.forEach(itemsModelEvent -> {
                                             long gmtCreate = itemsModelEvent.getGmtCreate();
                                             if (gmtCreate < timestamp) {
                                                 // 将水位线没过的记录排序。根据set的特性，如果排序字段(此处为事件时间)相同，只会保留其中一条。
                                                 outWatermarkItemsModelEventSet.add(itemsModelEvent);

                                             } else {
                                                 // 记录水位线内的list，最后更新到状态中
                                                 inWatermarkItemsModelEventList.add(itemsModelEvent);
                                             }
                                         }
                                 );

                                 // 通过比对填充eventOriValue值并输出下游。最后将排序中最新一条记录和未超过水位线的记录重新放入状态中(保证状态中有数据)
                                 ItemsModelEventDTO itemsModelEventDTOLag = null;
                                 Iterator<ItemsModelEventDTO> outWatermarkItemsModelEventSetIter = outWatermarkItemsModelEventSet.iterator();
                                 while (outWatermarkItemsModelEventSetIter.hasNext()) {
                                     ItemsModelEventDTO itemsModelEventDTO = outWatermarkItemsModelEventSetIter.next();
                                     // 如果是第一条，则不对比；从第二条开始和前一条对比(对应下面代码中存入一条，因为超过水位线为迟到数据，所以存入的一条肯定是最大的，即不会去对比上一条导致重复输出)
                                     if (Objects.isNull(itemsModelEventDTOLag)) {
                                         itemsModelEventDTOLag = itemsModelEventDTO;
                                         // 输出流中的第一条记录(事件时间最小)，即记录 从无到有 的变化
                                         if (Objects.isNull(isFirstFlagState.value())) {
                                             isFirstFlagState.update(false);
                                             out.collect(itemsModelEventDTO);
                                         }
                                     } else {
                                         // 当前数据
                                         String eventName = itemsModelEventDTO.getEventName();
                                         String eventValue = itemsModelEventDTO.getEventValue();

                                         String eventValueCode = null;
                                         String[] events = eventValue.split(",");
                                         for (String event : events) {
                                             String[] split = event.split(":");
                                             if (split[0].equals(eventName)) {
                                                 // 如果切分出来的value为空，直接使用split[1]会报错，这里判断一下
                                                 if (split.length == 1) {
                                                     eventValueCode = "";
                                                 } else {
                                                     eventValueCode = split[1];
                                                 }
                                                 break;
                                             }
                                         }

                                         // 前一条数据
                                         String eventOriValueCode = null;
                                         String eventValueLag = itemsModelEventDTOLag.getEventValue();
                                         String[] eventsLag = eventValueLag.split(",");
                                         for (String event : eventsLag) {
                                             String[] split = event.split(":");
                                             if (split.length == 1) {
                                                 eventOriValueCode = "";
                                             } else {
                                                 eventOriValueCode = split[1];
                                             }
                                             if (split[0].equals(eventName) && !eventOriValueCode.equals(eventValueCode)) {
                                                 // 如果相同事件的值不同，则将上一条的值放到本条数据的EventOriValue字段中
                                                 itemsModelEventDTO.setEventOriValue(eventValueLag);
                                                 out.collect(itemsModelEventDTO);
                                                 break;
                                             }
                                         }
                                         // 将当前数据赋值给Lag数据
                                         itemsModelEventDTOLag = itemsModelEventDTO;
                                     }

                                     // 最后一条还需要放入到状态后端中，作为新来的数据的比对象(对应前面代码中忽略第一条)
                                     if (!outWatermarkItemsModelEventSetIter.hasNext()) {
                                         inWatermarkItemsModelEventList.add(itemsModelEventDTO);
                                     }

                                 }

                                 // 更新到状态中
                                 listState.update(inWatermarkItemsModelEventList);
                             }
                         }
                );

        // 存储主流数据
        Properties kafkaSinkProperties = new Properties();
        kafkaSinkProperties.setProperty("bootstrap.servers", Constants.SINK_KAFKA_BOOTSTRAP_SERVERS);
        // 客户端事务的超时时间，超过了broker端允许的最大值
        /**
         默认broker端： transaction.max.timeout.ms=15min
         客户端的超时时间：不允许超过这个值，而它的默认： transaction.timeout.ms = 1h
         两个解决方法：
         ①在客户端：pros.put(“transaction.timeout.ms”, 15 * 60 * 1000);（小于15min）
         ②在服务端：修改配置文件：transaction.max.timeout.ms设置超过1小时 **/
        kafkaSinkProperties.put("transaction.timeout.ms", 15 * 60 * 1000);

        KafkaSerializationSchema<ItemsModelEventDTO> myMainkafkaSchema = new KafkaSerializationSchema<ItemsModelEventDTO>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(ItemsModelEventDTO element, @Nullable Long timestamp) {
                return new ProducerRecord<>("dwd_device_feiyan_log_event", (element.getProductKey() + "-" + element.getDeviceName()).getBytes(), JSONObject.toJSONBytes(element));
            }
        };
        FlinkKafkaProducer<ItemsModelEventDTO> myMainProducer = new FlinkKafkaProducer<ItemsModelEventDTO>(
                "dwd_device_feiyan_log_event",
                myMainkafkaSchema,
                kafkaSinkProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        resultDataStream.addSink(myMainProducer);

        // 存储迟到数据
        KafkaSerializationSchema<ItemsModelEventDTO> myLatekafkaSchema = new KafkaSerializationSchema<ItemsModelEventDTO>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(ItemsModelEventDTO element, @Nullable Long timestamp) {
                return new ProducerRecord<>("dwd_device_feiyan_log_late_event", (element.getProductKey() + "-" + element.getDeviceName()).getBytes(), JSONObject.toJSONBytes(element));
            }
        };
        FlinkKafkaProducer<ItemsModelEventDTO> myLateProducer = new FlinkKafkaProducer<ItemsModelEventDTO>(
                "dwd_device_feiyan_log_late_event",
                myLatekafkaSchema,
                kafkaSinkProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        resultDataStream.getSideOutput(lateOutputTag).addSink(myLateProducer);

        env.execute();
    }
}
