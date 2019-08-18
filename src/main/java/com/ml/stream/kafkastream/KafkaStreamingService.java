package com.ml.stream.kafkastream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


import java.util.HashMap;
import java.util.Map;
//import com.typesafe.config.ConfigFactory;
import java.util.Properties;


import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;

import com.ml.stream.kafkastream.serde.GenericAvroSerde;
import com.ml.stream.kafkastream.constants.*;


/**
 * @author Umesh.Menon
 * This class starts a streaming application. The streaming application reads the data from an 
 * input topic, calls the scorer class and stores the results back to an output topic. 
 * The code is generic and can be used irrespective of the task it executes (e.g. scoring in
 * our case). However since the objective of designing the class is mainly to help implement a 
 * scoring task within a Kafka Stream, the design is done accordingly. For e.g.Though Kafka 
 * supports multiple processors, for simplicity sake this class takes only one processor 
 * which is our scorer.
 */
public class KafkaStreamingService {
	private static Logger logger = LoggerFactory.getLogger(KafkaStreamingService.class.getSimpleName());
	
    public Properties appProps;
    private int threadCount=10;
    private Processor<String, GenericRecord> processor = null;
	private String source = "";
	private String sink = "";
	
	//private static ExecutorService executor;
	//private static String configFile;
    //public static Logger logger;
    //private static Properties kafkaProps;
	
    /**
     * Constructor takes a configuration
     */
    public KafkaStreamingService(Properties appProps){
    	this.appProps = appProps;
        this.initialize();
    }

    /**
     * Initializes the class variables
     */
    public void initialize(){
    	// check if threadCount is provided
    	this.threadCount = Integer.parseInt(this.appProps.getProperty(Constants.KAFKA_THREAD_COUNT, (String.valueOf(this.threadCount))));
    	this.source = this.appProps.getProperty(Constants.SRC_TOPIC);
    	this.sink = this.appProps.getProperty(Constants.OUT_TOPIC);
    }
   
    /**
     * Adds the source topic
     * @param src
     * @return
     */
    public KafkaStreamingService addSource(String src){
		this.source = src;
		return this;
	}
    
    /**
     * Adds Sink topic
     * @param snk
     * @return
     */
    public KafkaStreamingService addSink(String snk){
		this.sink = snk;
		return this;
	}
    
    /**
     * Sets the thread count in case we want parallel multi-threaded execution using executors
     * @param tCount
     */
    public KafkaStreamingService setThreadCount(int tCount){
    	this.threadCount = tCount;
    	return this;
    }
    
    /**
     * Does basic validations
     * @return
     */
    private Boolean validate(){
    	if (this.source == null || this.source.equals("")){
    		logger.error("Source topic must be provided");
    		return false;
    	}
    	
    	if (this.sink == null || this.sink.equals("")){
    		logger.error("Sink topic must be provided");
    		return false;
    	}
    	
    	return true;
    }
    
    /**
     * Override this method if you want to do any pre-processing transformation on your 
     * streaming data
     * @param stream
     * @return
     */
    public KStream<String, GenericRecord> preProcess(KStream<String, GenericRecord> stream){
    	return stream;
    }
    
    /**
     * Override this method if you want to do any post-processing transformation on your 
     * streaming data
     * @param stream
     * @return
     */
    public KStream<String, GenericRecord> postProcess(KStream<String, GenericRecord> stream){
    	return stream;
    }
    
    
    /**
     * Main function that consumes the Kafka topic and executes a stream processor
     */
    public void run(){
        logger.info("Starting the service...");
        if (this.validate()){
        	final StreamsBuilder builder = new StreamsBuilder();
            KStream<String, GenericRecord> scorerStream = builder
                    .stream(this.source);
            // TODO: We do not want executor here as the job can be done using in a single thread
            //executor = Executors
            //        .newFixedThreadPool(this.threadCount);
            logger.info("Started reading the records from source topic: " + this.source + "...");
            scorerStream = this.preProcess(scorerStream)
            		//.transform(() -> new ABTestingTransformer(this.scorer.getModels())) // moved to scorer service
                    //.foreach((key, value) -> executor.submit(() -> (this.task(value))));
            		.mapValues((key, value) -> this.task(value));
            		//.transform(new TimestampTransformerSupplier());	
            // foreach is a terminal operation and in this case the task() was writing to an
            // output topic using Kafka Producer. We are changing it to mapvalues() which let 
            // the stream continue and then using .to() method to write to output topic
            scorerStream = this.postProcess(scorerStream);
            // Send prediction information to Output Topic
            scorerStream.peek((k, v)->logger.debug("Final value to output topic: {}", v))
            .to(this.sink, Produced.with(getOutKeySerde(), getOutValueSerde()));
            
            final KafkaStreams streams = new KafkaStreams(builder.build(), this.appProps);
            //streams.cleanUp();
            streams.start();
            logger.info("Started ComponentScore streaming....");
            // Add shutdown hook to respond to SIGTERM and gracefully close Kafka
            // Streams
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
        
    }
    
    /**
     * Create a KeySerde. Just to make sure the output topic is created with right schema. REVISIT.
     * @return
     */
    private Serde getOutKeySerde(){
    	GenericAvroSerde outSerde = new GenericAvroSerde();
    	outSerde.serializer().configure(kStreamMapConfig(), true);
    	outSerde.deserializer().configure(kStreamMapConfig(), true);
    	return outSerde;
    }
    
    /**
     * Just to make sure the output topic is created with right schema. REVISIT.
     * @return
     */
    private Serde getOutValueSerde(){
    	GenericAvroSerde outSerde = new GenericAvroSerde();
    	outSerde.serializer().configure(kStreamMapConfig(), false);
    	outSerde.deserializer().configure(kStreamMapConfig(), false);
    	return outSerde;
    }
    
    /**
     * Just to make sure the output topic is created with right schema. REVISIT.
     * @return
     */
    private Map<String, Object> kStreamMapConfig() {
	    Map<String, Object> config = new HashMap<String, Object>();   
	    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.appProps.getProperty(Constants.SCHEMA_REGISTRY_URL));
	    return config;
    }

    /**
     * Override this task method in case you want to add more processing logic. 
     *
     */
    public GenericRecord task(GenericRecord value){
        return value;
    }

}

