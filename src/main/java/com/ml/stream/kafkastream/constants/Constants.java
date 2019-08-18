package com.ml.stream.kafkastream.constants;

/**
 * 
 * @author Umesh.Menon
 *	This class contains all the constants used in this project
 */
public class Constants {
    //kafka configuration constants
    public static final String SRC_TOPIC = "source.topic";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.server";
    public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schema.registry";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String KAFKA_CONSUME_POLICY = "kafka.consume.policy";
    public static final String KAFKA_PRODUCER_ACK = "acks";
    public static final String KAFKA_PRODUCER_RETRIES = "retries";
    public static final String KAFKA_THREAD_COUNT = "thread.count";
    public static final String STREAM_APP_ID = "stream.app.id";

    public static final String INPUT_TOPIC_SCHEMA = "input.topic.schema";
    public static final String INPUT_TOPIC_SCHEMA_CLASSNAME = "input.topic.schema.classname";
    public static final String INPUT_TOPIC_SCHEMA_MODEL_CLASSNAME = "input.schema.modelClassName";
    public static final String OUT_TOPIC = "producer.topic";
    public static final String OUT_TOPIC_PREDICTION_COLUMN = "out.topic.pred.column";
    public static final String FEATURES_DEF = "features_def";
    public static final String FEATURE_MAPPER = "feature_mapper";
    public static final String LOGGER_TOPIC = "logger.topic";
    public static final String APPLICATION_ID = "application.id";
    public static final String ROW_IDENTIFIER = "row.identifier";
    public static final String PREDICTION_OUTPUT_TYPE = "prediction.output.type";
    
    public static final String MODEL_UUID = "model_uuid";
    public static final String MODEL_TAG = "model_tag";
    
    // whether the records can be forwarded or not
    public static final String CAN_FORWARD = "_can_forward";
    
    // Output Schema
    public static final String OUTPUT_SCHEMA_NAMESPACE = "com.ml.kafka.outputschema.ns";
    public static final String OUTPUT_SCHEMA_NAME = "output-schema";
    public static final String OUTPUT_PREDICTION_LOG_SCHEMA_NAME = "prediction.log.schema";
    public static final String COLUMN_ROW_ID = "_id";
    public static final String COLUMN_PREDICTION = "_prediction";
    public static final String COLUMN_TIMESTAMP = "_timestamp";
    public static final String COLUMN_MODEL_UUID = "_model_uuid";
    public static final String COLUMN_MODEL_TAG = "_model_tag";
    public static final String COLUMN_MODEL_FEATURES = "_model_features";
    
    public static final String VALUE_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    public static final String DESERIALIZATION_EXCEPTION_HANDLER = "DeserializationExceptionHandler";
}
