package com.ml.stream.kafkastream.stream;

import java.util.Properties;

import com.ml.stream.kafkastream.constants.Constants;
import com.ml.stream.kafkastream.model.PredictValueType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.ml.stream.kafkastream.model.Prediction;
import com.ml.stream.kafkastream.model.RowData;
import com.ml.stream.kafkastream.utils.AvroUtil;

/**
 * @author Umesh.Menon
 * We need to enforce the sub classes to set the schema classes and to predict. Hence this AbstractClass!
 */
public abstract class AbstractStreamScorer {
	protected Schema inputSchema;
	protected String inputSchemaClassName;
    protected Schema outputSchema;
    protected String outputSchemaClassName;
    protected PredictValueType predictValueType;
    protected String currentModelUUID;
    protected String currentModelTag;
    protected RowData modelFeatures;
    protected String outputTopic;
    protected String rowIdentifier = null;
    protected String predictColumn = null;
    protected String featureMapper = null;
    protected String schemaRegistryUrl = null;
    protected Properties props;
    
	public AbstractStreamScorer(Properties properties){
		// we will enforce the subclasses to set the model id and tag so that we will know
		// which model has done the prediction
		props = properties;
    	this.initialize();
	}
	
	/**
	 * Initializes the variables
	 */
	private void initialize(){
		outputTopic = props.getProperty(Constants.OUT_TOPIC);
    	rowIdentifier = props.getProperty(Constants.ROW_IDENTIFIER);
    	predictColumn = props.getProperty(Constants.OUT_TOPIC_PREDICTION_COLUMN);
    	featureMapper = props.getProperty(Constants.FEATURE_MAPPER);
    	schemaRegistryUrl = props.getProperty(Constants.SCHEMA_REGISTRY_URL);
    	this.outputSchema = AvroUtil.getAvroSchema(outputTopic, schemaRegistryUrl);
    	predictValueType = PredictValueType.valueOf(props.getProperty(Constants.PREDICTION_OUTPUT_TYPE).toUpperCase());
    	//predictValueType = setPredictValueType();
		//inputSchema = setInputSchema();
		//inputSchemaClassName = setInputSchemaClassName();
		//outputSchemaClassName = setOutputSchemaClassName();
		
	}
	
	//public abstract Schema setInputSchema();
	
	//public abstract String setInputSchemaClassName();
	
	//public abstract Schema setOutputSchema();
	
	//public abstract String setOutputSchemaClassName();
	
	//public abstract PredictValueType setPredictValueType();
	
	public void setCurrentModelTag(String value){
		this.currentModelTag = value;
	}
	
	public void setCurrentModelUUID(String value){
		this.currentModelUUID = value;
	}
	
	//public abstract Object score(HashMap<String, Object> rowData);
	public abstract Object score(RowData rowData);
	
	protected abstract GenericRecord createOutputRecord(GenericRecord value, Prediction pred);
}
