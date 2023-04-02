package com.iotmars.compass.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * @author CJ
 * @date: 2023/3/30 2:35
 */
@PublicEvolving
public class JSONKeyValueDeserializationSchema implements KafkaDeserializationSchema<ObjectNode> {
    private static final Logger logger = LoggerFactory.getLogger(com.iotmars.compass.util.JSONKeyValueDeserializationSchema.class);

    private static final long serialVersionUID = 1509391548173891955L;

    private final boolean includeMetadata;
    private ObjectMapper mapper;

    public JSONKeyValueDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode node = mapper.createObjectNode();
        if (record.key() != null) {
            node.set("key", mapper.readValue(record.key(), JsonNode.class));
        }
        if (record.value() != null) {
            try {
                node.set("value", mapper.readValue(record.value(), JsonNode.class));
            } catch (Exception e) {
                logger.error("解析JSON发生错误: " + new String(record.value()));
            }
        }
        if (includeMetadata) {
            node.putObject("metadata")
                    .put("offset", record.offset())
                    .put("topic", record.topic())
                    .put("partition", record.partition());
        }
        return node;
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }
}

