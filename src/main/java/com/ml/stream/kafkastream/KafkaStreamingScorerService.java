package com.ml.stream.kafkastream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//import com.typesafe.config.ConfigFactory;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.streams.kstream.KStream;

import com.ml.stream.kafkastream.stream.StreamScorer;
import com.ml.stream.kafkastream.transformers.ABTestingTransformer;


/**
 * @author Umesh.Menon
 * This class starts a streaming application. The streaming application reads the data from an 
 * input topic, calls the scorer class and stores the results back to an output topic. 
 * Though Kafka supports multiple processors, for simplicity sake this class takes only one 
 * processor which is our scorer.
 */
public class KafkaStreamingScorerService extends KafkaStreamingService{
	private static Logger logger = LoggerFactory.getLogger(KafkaStreamingScorerService.class.getSimpleName());
	
	protected StreamScorer scorer = null;
	
    /**
     * Constructor takes a configuration
     */
    public KafkaStreamingScorerService(Properties appProps, StreamScorer scorer){
    	super(appProps);
    	this.scorer = scorer;
    }
    
    /**
     * Add all the pre-processing transformation on the streaming data here
     */
    @Override
    public KStream<String, GenericRecord> preProcess(KStream<String, GenericRecord> stream){
    	// #1: Transformer 1: AB Testing Transformer. Filters in records only if the model uuid
    	// or tag matches if provided.
    	stream = stream.transform(() -> new ABTestingTransformer(this.scorer.getModels()));
    	return stream;
    }
    
    /**
     * Does the actual job
     */
    @Override
    public GenericRecord task(GenericRecord value){
    	logger.info("Loading the specific task...");
    	GenericRecord result = this.scorer.run(value);
        return result;
    }

}
