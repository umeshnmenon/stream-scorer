package com.ml.stream.kafkastream.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.ml.stream.kafkastream.constants.Constants;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

import com.ml.stream.kafkastream.model.RowData;
import com.ml.stream.kafkastream.utils.AvroUtil;
import com.ml.stream.kafkastream.utils.CoreUtils;

/**
 * @author Umesh.Menon
 * This class builds output record to insert into a Kafka topic dynamically
 */
public class OutputRecord {
	
	/**
	 * Build a GenericRecord from another GenericRecord
	 * @param schema
	 * @param value
	 * @return
	 */
	public static <T extends HashMap<String, Object>> GenericRecord build(Schema schema, GenericRecord value, T metadata){
		GenericRecord outRecord = null;
		outRecord = AvroUtil.createRecordFromAnother(schema, value);
		outRecord = AvroUtil.addValuesToGenericRecord(outRecord, metadata);
		return outRecord;
	}
	
	/**
	 * Build a GenericRecord from a default map of key->value pairs. This map contains the metadata.
	 * @param values
	 * @return
	 */
	//public static <T extends HashMap<String, Object>> GenericRecord build(T values, String topic){
	public static GenericRecord build(HashMap<String, Object> values, String topic){
		GenericRecord outRecord = null;
		Schema outSchema = buildOutputSchema(topic);
		outRecord = AvroUtil.createGenericRecordWithSchema(outSchema, values);
		return outRecord;
	}
	
	/**
	 * Build a GenericRecord from a default map of key->value pairs (metadata) and from a map of features.
	 * @param values
	 * @param features
	 * @return
	 */
	//public static <T extends HashMap<String, Object>> GenericRecord build(T values, T features, String topic){
	public static GenericRecord build(HashMap<String, Object> values, RowData features, String topic){
		GenericRecord outRecord = null;
		Schema outSchema = buildOutputSchema(features, topic);
		// combine both values and features
		HashMap<String, Object> all = CoreUtils.combineMaps(values, features);
		outRecord = AvroUtil.createGenericRecordWithSchema(outSchema, all);
		return outRecord;
	}
	
	/**
     * Creates an output schema dynamically from a given schema
     * @param base
     * @return
     */
    public static Schema buildOutputSchema(Schema base, String topic){
    	// Get a copy of base schema's fields.
    	// Once a field is used in a schema, it gets a position.
    	// We can't recycle a field and it will throw an exception.
    	// Hence, we need a fresh field from each field of the old schema
    	List<Schema.Field> baseFields = base.getFields().stream()
    	            .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
    	            .collect(Collectors.toList());
    	// Add your field
    	baseFields.add(new Schema.Field(Constants.COLUMN_ROW_ID, Schema.create(Schema.Type.STRING), Constants.COLUMN_ROW_ID, null));
    	baseFields.add(new Schema.Field(Constants.COLUMN_PREDICTION, Schema.create(Schema.Type.STRING), Constants.COLUMN_PREDICTION, null));
    	baseFields.add(new Schema.Field(Constants.COLUMN_MODEL_UUID, Schema.create(Schema.Type.STRING), Constants.COLUMN_MODEL_UUID, null));
    	baseFields.add(new Schema.Field(Constants.COLUMN_MODEL_TAG, Schema.create(Schema.Type.STRING), Constants.COLUMN_MODEL_TAG, null));
    	baseFields.add(new Schema.Field(Constants.COLUMN_TIMESTAMP, Schema.create(Schema.Type.STRING), Constants.COLUMN_TIMESTAMP, null));
    	Schema outputSchema = Schema.createRecord(
    	    base.getName(), 
    	    "System Generated Schema for Output Record", 
    	    Constants.OUTPUT_SCHEMA_NAMESPACE, 
    	    false, 
    	    baseFields);
    	
    	return outputSchema;
    }
    
    /**
     * Creates an output schema dynamically from a given schema
     * @param topic
     * @return
     */
    public static Schema buildOutputSchema(String topic){
    	Schema outputSchema = SchemaBuilder
                .record(topic)
                .namespace(Constants.OUTPUT_SCHEMA_NAMESPACE)
                .fields()
                .name(Constants.COLUMN_ROW_ID).type().stringType().noDefault() //.optional()
                .name(Constants.COLUMN_PREDICTION).type().stringType().noDefault()
                .name(Constants.COLUMN_MODEL_UUID).type().stringType().noDefault()
                .name(Constants.COLUMN_MODEL_TAG).type().stringType().noDefault()
                .name(Constants.COLUMN_TIMESTAMP).type().stringType().noDefault()
                .endRecord();
   
    	return outputSchema;
    }
    
    /**
     * Creates an output schema dynamically from a given schema
     * @param features
	 * @param topic
     * @return
     */
    public static <T extends HashMap<String, Object>> Schema buildOutputSchema(T features, String topic){
    	// Add features
    	List<Field> outputFields = new ArrayList<Field>();
    	// add row id first
    	outputFields.add(new Field(Constants.COLUMN_ROW_ID, Schema.create(Schema.Type.STRING), Constants.COLUMN_ROW_ID, "-1"));
    	for (Map.Entry mapElement : features.entrySet()) { 
            String key = (String)mapElement.getKey(); 
            outputFields.add(new Field(key, Schema.create(Schema.Type.STRING), key, "-1"));
        }
    	
    	// Add other useful info
    	outputFields.add(new Field(Constants.COLUMN_PREDICTION, Schema.create(Schema.Type.STRING), Constants.COLUMN_PREDICTION, "-1"));
    	outputFields.add(new Field(Constants.COLUMN_MODEL_UUID, Schema.create(Schema.Type.STRING), Constants.COLUMN_MODEL_UUID, "-1"));
    	outputFields.add(new Field(Constants.COLUMN_MODEL_TAG, Schema.create(Schema.Type.STRING), Constants.COLUMN_MODEL_TAG, "-1"));
    	outputFields.add(new Field(Constants.COLUMN_TIMESTAMP, Schema.create(Schema.Type.STRING), Constants.COLUMN_TIMESTAMP, "-1"));
    	
    	Schema outputSchema = Schema.createRecord(
    			topic, 
        	    "System Generated Schema for Output Record", 
        	    Constants.OUTPUT_SCHEMA_NAMESPACE, 
        	    false, 
        	    outputFields);

    	return outputSchema;
    }
	
}
