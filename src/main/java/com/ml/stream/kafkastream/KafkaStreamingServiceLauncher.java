package com.ml.stream.kafkastream;

import java.util.Properties;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ml.stream.kafkastream.constants.Constants;
import com.ml.stream.kafkastream.exception.handlers.DeserializationExceptionHandler;
import com.ml.stream.kafkastream.serde.GenericAvroSerde;
import com.ml.stream.kafkastream.stream.StreamScorer;
import com.ml.stream.kafkastream.utils.AvroUtil;
import com.ml.stream.kafkastream.utils.PropertiesUtil;

/**
 * @author Umesh.Menon
 * This class will lanuch the Kafka Streaming Scorer Service.
 * service
 */
public class KafkaStreamingServiceLauncher{
	 
	 /**
	 * @param args This method is entry point for the application
	 */
    public static void main(String[] args) {
    	
    	/*
    	* // Uncomment the below code block after creating specific classes for your use case.
    	* // Refer git README.
      
    	* // Call the boot strap to set the Properties because both Scorer and Scorer service will need
    	* // the properties
    	* // We can easily read the properties in Scorer and ScorerService class and avoid 
    	* // the below code, but that may not be a good design.
    	* 
    	* Properties props = KafkaStreamingServiceBootstrap(args).getAppProperties();
    	
    	* // create the model
        * Model model = new H2OModel("GLM_model_R_1", "GLM_Test_Model", "GLM_model_R_1");

        * // create your scorer processor
        * StreamScorer scorer = new StreamScorer(props, model);

    	* // start the service
	    * KafkaStreamingScorerService scorerService = new KafkaStreamingScorerService(props, scorer);
	    * scorerService.addScorer(this.scorer);
	    * scorerService.run();
	     
	     */
    }
}
