package com.ml.stream.kafkastream.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericRecord;

import com.ml.stream.kafkastream.constants.Constants;

import java.util.Properties;

/**
 * @author Umesh.Menon
 * The class takes care of writing records to a Kafka topic
 */
public class KafkaWriter {

    private Properties props;
    private String topic;
    private KafkaProducer<String, GenericRecord> producer;
    private static Logger logger = LoggerFactory.getLogger(KafkaWriter.class.getSimpleName());

    public KafkaWriter(String topic, Properties props){
        this.props = props;
        this.topic = topic;
        this.initialize();
        this.producer = this.getKafkaProducer();
    }

    /**
     * All the class variable initialization will go here
     */
    private void initialize(){
    	// for testing
    	//this.props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true); //it's already true by default
    	//this.props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.props.getProperty(Constants.SCHEMA_REGISTRY_URL)); // this is already set
    	this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.VALUE_SERIALIZER_CLASS);
    	this.props.put("batch.size", 1);
    	this.props.put("default.deserialization.exception.handler", Constants.DESERIALIZATION_EXCEPTION_HANDLER);
    }
    
    /**
     * Writes the record to a topic
     */
    public void write(GenericRecord record){
        logger.debug("Record to be insert: {}", record);
        ProducerRecord<String, GenericRecord> producerRecord =
                new ProducerRecord<>(this.topic, record);
        logger.debug("Writing to topic: {}", this.topic);
        this.producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                logger.debug("Failed to write record {} with exception {}", record, exception);
            }
        });
        this.producer.flush();
    }

    private KafkaProducer<String, GenericRecord> getKafkaProducer() {
        logger.debug("Properties for Kafka Producer", this.props);
        return new KafkaProducer<>(this.props);

    }
}
