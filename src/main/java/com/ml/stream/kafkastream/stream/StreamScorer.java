package com.ml.stream.kafkastream.stream;

import com.ml.stream.kafkastream.constants.Constants;

import com.ml.stream.kafkastream.kafka.KafkaWriter;
import com.ml.stream.kafkastream.record.OutputRecord;
import com.ml.stream.kafkastream.utils.CoreUtils;
import com.ml.stream.kafkastream.utils.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
//import com.typesafe.config.ConfigFactory;
import org.apache.avro.Schema;

//import hex.genmodel.GenModel;
//import hex.genmodel.easy.EasyPredictModelWrapper;
//import hex.genmodel.easy.RowData;
//import hex.genmodel.easy.exception.PredictException;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ml.stream.kafkastream.record.LogRecord;
import com.ml.stream.kafkastream.utils.*;
import com.ml.stream.kafkastream.model.Model;
import com.ml.stream.kafkastream.model.Prediction;
import com.ml.stream.kafkastream.model.RowData;
import com.ml.stream.kafkastream.utils.AvroUtil;

/**
 * @author Umesh.Menon
 * This class is responsible for prediction. It takes the input record and converts it to a 
 * specific record of given schema, pushes it to the model and dispatches the result.
 */
public class StreamScorer extends AbstractStreamScorer implements IStreamProcessor, IStreamScorer{

    private static Logger logger = LoggerFactory.getLogger(StreamScorer.class.getSimpleName());
     
    protected ArrayList<Model> models;
    private KafkaWriter writer;
	private String logTopic;
	private String schemaRegistryUrl;
	private Schema logSchema = null;
	private Model model = null;
    
    /**
     * Singleton Constructor
     */
    /*
    private StreamScorer(Properties props){
        this.props = props;
        this.initialize();
    }

    public static StreamScorer getInstance(Properties props){
        if(instance == null){
            instance = new StreamScorer(props);
        }
        return instance;
    }
    */
    
	/**
	 * A constructor with model. This will be useful in case if we want everything to be taken
	 * care of by the base Scorer class.
	 * Properties is mainly needed for Kafka connector to update Model Orchestration tables
	 * @param properties
	 * @param model
	 */
    public StreamScorer(Properties properties, Model model){
    	super(properties);
        this.initialize();
        this.model = model;
    	// push the model
        this.addModel(model);
    }
    
    /**
     * A constructor without model. This will be tidy in case we want to create
     * multiple models in the scorer class
     * @param properties
     */
    public StreamScorer(Properties properties){
    	super(properties);
        this.initialize();
    }
    
    /**
     * Initializes the class variables
     */
    public void initialize(){
    	//output schema would have been set by the super() initializer
    	logger.debug("Output Schema: {}", this.outputSchema);
    	// initialize the models store 
    	this.models = new ArrayList<>();
    	this.loadModels();
        this.createKafkaWriter();
    }
    
    /**
     * If we have more than one model, add here
     */
    protected void loadModels(){
    	//this.modelA = new Model();
    	//this.addModel(this.modelA);
    }

    /**
     * Create a KafkaWriter. This is mainly to write to log topic.
     */
    private void createKafkaWriter(){
    	logTopic = this.props.getProperty(Constants.LOGGER_TOPIC);
		schemaRegistryUrl = this.props.getProperty(Constants.SCHEMA_REGISTRY_URL);
		logger.info("Logging prediction to " + this.logTopic);
		logger.info("Getting the log schema from " + this.schemaRegistryUrl);
		try {
			// TODO: Pass this as an argument
			this.logSchema = AvroUtil.getAvroSchema(logTopic, schemaRegistryUrl);
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error("Error while fetching the ML Prediction Logs schema. Error: " + e.getMessage());
		}
		logger.debug("Log Schema: {}", this.logSchema);
    	this.writer = new KafkaWriter(props.getProperty(Constants.LOGGER_TOPIC), props);
    }
    
    // Some getter and setters
    public Schema getSchema() {
        return inputSchema;
    }

    public String getSchemaClassName(){
        return inputSchemaClassName;
    }
    
    public void setProperties(Properties props){
    	this.props = props;
    }
    
    
    public void setFeatureMapper(String value){
    	this.featureMapper = value;
    }
    
    public void setPredictionColumn(String value){
    	this.predictColumn = value;
    }
    
    public String getPredictionColumn(){
    	return this.predictColumn;
    }
    
    public void setRowIdentifier(String value){
    	this.rowIdentifier = value;
    }
    
    public StreamScorer addModel(Model model){
    	this.models.add(model);
    	return this;
    }
    
    public ArrayList<Model> getModels(){
    	return this.models;
    }
    
    /**
     * This function will execute all the steps to do a prediction
     */
    public GenericRecord run(GenericRecord value){
        RowData record = this.read(value);
        RowData features = this.preprocess(record);
        Prediction result = this.predict(features);
        GenericRecord response = prepareResponse(value, result);
        this.logPrediction(value, result);
        this.dispatch(response);
        logger.debug("Record for Output Topic: {}", response);
        return response;
    }

    /**
     * Loads the data from input topic, reads it as per the schema and returns a HashMap of column, value
     * pairs
     */
    public RowData read(GenericRecord value){
    	GenericRecord record = null;
        logger.info("Reading the record from input topic....");
        record = this.createInputRecord(value);
        logger.info("Input record is parsed and read to specific schema.");
        // load only the features that are needed for the model
        RowData rowData = (RowData) this.loadFeatures(record);
        // set modelFeatures so that we can use that later while creating output record in createOutputRecord
        this.modelFeatures = rowData;
        return rowData;
    }

    /**
     * Gets the specific record. Converts the Generic Record to a Specific Record of given schema
     * @param value
     * @return
     */
    protected GenericRecord createInputRecord(GenericRecord value){
    	// TODO: Check if we really need to do this. 
    	//return InputRecord.build(value, inputSchema);
    	return value;
    }
    
    /**
     * Prepared the input features to the model. `value` is the row loaded from Kafka Topic and `features` are name of the features
     * mentioned in the `feature_mapper` in application.properties
     * @param value
     * @return
     */
    //public <T> HashMap<String, Object> loadFeatures(GenericRecord value){
    //public Object loadFeatures(GenericRecord value){
    public RowData loadFeatures(GenericRecord value){
        logger.info("Loading the features from the input record based on feature mapper table...");
        RowData row = null;
        if (this.featureMapper == null || this.featureMapper.equals("")){
        	// if features are not provided, just load the data from input record
        	row = DataUtil.populateRow(value);
        }else{
        	Map<String, String> featureMapper = CoreUtils.jsonToMap(this.featureMapper);
            row = DataUtil.populateRow(value, featureMapper);
        }
        logger.info("Features are loaded successfully.");
        //logger.debug("Loaded Features are: " + featureList);
        return row;
    }
    
    /**
     * Add any preprocessing logic to this pipeline
     */
    //public <T extends GenericRecord> T preprocess(){
    public RowData preprocess(RowData record){
        //logger.info("Preprocessing records...");
        // Add any custom logic that you want to write by extending this function. The function must return a GenericRecord
        return record;
    }

    /**
     * The class exposes the predict method which loads the model object and does the prediction
     * This is created for method overloading.
     */
    public Prediction predict(RowData rowData){
        logger.info("Predicting the score...");
        return this._predict(rowData);
    }

    /**
     * The class exposes the predict method which loads the model object and does the prediction
     * Providing model gives the flexibility to call multiple models from the same consumer class.
     * This is created for method overloading.
     */
    public Prediction predict(RowData rowData, Model model){
        this.model = model;
        return this._predict(rowData);
    }

    /**
     * Predict function
     */
    @SuppressWarnings("finally")
	private Prediction _predict(RowData rowData){
        Prediction predictScore = null;
        try {
        	// For runtime A/B testing
        	// check if the model id is given in the record header or not
            logger.debug("Predicting score for row data :", rowData);
            predictScore = this.score(rowData);
            logger.debug("Predicting score for row data {}: is {}", rowData, predictScore);
        } catch (Exception e) {
            logger.error("Error in Predicting score for row data {}:{}", rowData, e);
        }
        finally {
            return predictScore;
        }
    }

    // Any subclass that extends this class must override this method
    /**
     * The actual prediction function. All the specific classes must implement their own predict 
     * function.
     */
    @Override
    //public Prediction score(HashMap<String, Object> rowData){
    public Prediction score(RowData rowData){
    	// by default, this.models will have only one item, so use that to predict
    	Model currentModel = this.models.get(0); // or this.model
    	return this._score(rowData, currentModel);
    }
    
    /**
     * Does the prediction and also sets the model id and tag, so that it will be used for 
     * writing to output topic and log topic
     * @param rowData
     * @param currentModel
     * @return
     */
    protected Prediction _score(RowData rowData, Model currentModel){
    	// set the model uuid and tag
    	// TODO: Change this implementation
    	setCurrentModelUUID(currentModel.getModelUUID());
    	setCurrentModelTag(currentModel.getModelTag());
        return currentModel.predict(rowData);
    }
    

    /**
     * Override this method to dispatch the response 
     * By default, we will write back to another topic if provided in the config 
     * @param input
     * @param response
     */
    public void dispatch(GenericRecord response){
    	 //this.write(response);
    	// Write method is moved to KafkaStreamingScorerService using DSL .to() method
    }
    
    /**
     * Logs the prediction to a prediction log topic
     * @param response
     */
    public void logPrediction(GenericRecord input, Prediction pred){
    	try{
    		// first create the log record
    		GenericRecord logRecord = createLogRecord(input, pred);
            logger.info("Writing the prediction output to destination Kafka topic");
            this.writer.write(logRecord);
            logger.info("Successfully written prediction output to output topic.");
        }
        catch (Exception e){
            logger.error("Error while writing record to Kafka topic. Error: {}", e.toString());
        }
    }
    
    /**
     * 
     * Writes the prediction back to Kafka Output Topic
     */
    public void write(GenericRecord response){
        try{
            logger.info("Writing the prediction output to destination Kafka topic");
            KafkaWriter writer = new KafkaWriter(props.getProperty(Constants.OUT_TOPIC), props);
            writer.write(response);
            logger.info("Successfully written prediction output to output topic.");
        }
        catch (Exception e){
            logger.error("Wrror while writing record to Kafka topic. Error: {}", e.toString());
        }
    }
    
    /**
     * Wraps the predicted value as a GenericRecord
     * @return
     */
    public GenericRecord prepareResponse(GenericRecord input, Prediction pred){
    	GenericRecord response = null;
    	// Get the output schema
    	// check if the schema exists
    	if (this.outputSchema != null){// This means the user has provided the output schema
    		response = createOutputRecord(input, pred);
    	}else{ // if not create a new topic to put the predicted value and features
    		logger.info("Output schema is not available. Creating a default record");
    		response = createDefaultOutputRecord(input, pred);
    	}
    	return response;
    }
    
    @Override
    protected GenericRecord createOutputRecord(GenericRecord value, Prediction pred){
    	return null;
    }
    
    /**
     * Creates the output record to be readily written to sink Kafka topic. By default, all the 
     * input features, unique identifier to identify the record, and prediction output are used
     * in the output record. If you would like to use a different schema for this, just override
     * this method.
     * @param value
     * @return
     */
    private GenericRecord createDefaultOutputRecord(GenericRecord value, Prediction pred){
    	logger.warn("Creating default output record. Note that this will automtaically create a schema by the application which is not desirable");
    	HashMap<String, Object> values = getOutputColumns(value, pred);
    	GenericRecord outRecord = OutputRecord.build(values, this.modelFeatures, this.outputTopic);
    	return outRecord;
    }
    
    /**
     * Creates generic record to store to LogTopic
     * @param value
     * @param pred
     * @return
     */
    // TODO: ADD check to see if the topic exists and if not create it
    private GenericRecord createLogRecord(GenericRecord value, Prediction pred){
    	// Get the output schema
    	HashMap<String, Object> values = getOutputColumns(value, pred);
    	GenericRecord outRecord = LogRecord.build(values, this.modelFeatures, this.logSchema);
        return outRecord;
    }

    /**
     * Creates a map of output column values to be put in the output topic
     * @return
     */
    private HashMap<String, Object> getOutputColumns(GenericRecord input, Prediction pred){
    	HashMap<String, Object> values = new HashMap<String, Object>();
    	values.put(Constants.COLUMN_ROW_ID, String.valueOf(input.get(this.rowIdentifier)));
    	values.put(Constants.COLUMN_PREDICTION, getPredictedValue(pred).toString());
    	values.put(Constants.COLUMN_MODEL_UUID, this.currentModelUUID);
    	values.put(Constants.COLUMN_MODEL_TAG, this.currentModelTag);
    	long ts = CoreUtils.getCurrentTimestamp();
    	values.put(Constants.COLUMN_TIMESTAMP, ts);//new Date(ts) //String.valueOf(ts)
    	return values;
    }
    
    /**
     * Gets the value of interest from Prediction object
     * @param pred
     * @return
     */
    private Object getPredictedValue(Prediction pred){
    	Object ret = null;
    	switch (predictValueType){
		case LABEL:
			ret = pred.getLabel();
			break;
		case PROB0:
			ret = pred.getProb0();
			break;
		case PROB1:
			ret = pred.getProb1();
			break;
		case VALUE:
			ret = pred.getValue();
			break;
		default:
			ret = pred.getValue();
			break;
    	}
    	return ret;
    }
	
	public void setOutputSchema(Schema outSchema){
		if (this.outputSchema == null){ //outputSchema can be directly set by the sub classes when initializing, so doing a check here to make sure
			// we are not overriding a valid value with a null.
			this.outputSchema = outSchema;
		}
    }

	/*
	@Override
	public PredictValueType setPredictValueType() {
		// TODO: Change this implemenation
		return null;
	}
	*/
	
	/**
     * Adds default system generated columns to output record. OBSOLETE
     * @param outRecord
     * @param value
     * @param pred
     * @return
     */
    /*
    private GenericRecord addSystemGeneratedColumnsToOutputRecord(GenericRecord outRecord, GenericRecord value, Prediction pred){
    	// set the identifier
        // Get the identifier column value first from the input record
    	String id = String.valueOf(value.get(this.rowIdentifier));
    	outRecord.put(this.rowIdentifier, id);
        // Set the predicted value
        //outRecord.put(props.getProperty(Constants.OUT_TOPIC_PREDICTION_COLUMN), value);
        outRecord.put(this.predictColumn, getPredictedValue(pred));
        if (this.ifModelColumnExist(outputSchema)){
        	outRecord.put(Constants.MODEL_UUID, this.model.getModelUUID());
        	outRecord.put(Constants.MODEL_TAG, this.model.getModelTag());
        }
        return outRecord;
    }
    */
    
    /**
     * Checks if the output schema has a model column exists to update which model has produced
     * the prediction
     * @param schema
     * @return
     */
	/*
    private Boolean ifModelColumnExist(Schema schema){
    	for (Schema.Field field : schema.getFields()){
    		if (field.name() == Constants.MODEL_UUID || field.name() == Constants.MODEL_TAG){
    			return true;
    		}
        }
    	return false;
    }
    */
}
