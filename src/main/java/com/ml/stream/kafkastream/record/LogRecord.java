package com.ml.stream.kafkastream.record;

import java.util.HashMap;

import com.ml.stream.kafkastream.constants.Constants;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import com.ml.stream.kafkastream.utils.AvroUtil;
import com.ml.stream.kafkastream.utils.CoreUtils;

/**
 * @author Umesh.Menon
 * This class helps construct schema and record for logging the predictions
 */
public class LogRecord {
	/**
	 * Build a GenericRecord from a default map of key->value pairs and from a map of features.
	 * @param values
	 * @param features
	 * @return
	 */
	public static <T extends HashMap<String, Object>> GenericRecord build(T values, T features, Schema schema){
		GenericRecord outRecord = null;
		//Schema outSchema = buildOutputSchema();
		// construct json string from features map
		String featureJson = CoreUtils.mapToJson(features);
		// combine both values and features
		values.put(Constants.COLUMN_MODEL_FEATURES, featureJson);
		outRecord = AvroUtil.createGenericRecordWithSchema(schema, values);
		return outRecord;
	}
	
	/**
     * Creates an output schema dynamically from a given schema
     * @param base
     * @return
     */
    public static Schema buildOutputSchema(){
    	Schema outputSchema = SchemaBuilder
                .record(Constants.OUTPUT_PREDICTION_LOG_SCHEMA_NAME)
                .namespace(Constants.OUTPUT_SCHEMA_NAMESPACE)
                .fields()
                .name(Constants.COLUMN_ROW_ID).type().stringType().noDefault() //.optional()
                .name(Constants.COLUMN_MODEL_FEATURES).type().stringType().stringDefault(null)
                .name(Constants.COLUMN_PREDICTION).type().stringType().noDefault()
                .name(Constants.COLUMN_MODEL_UUID).type().stringType().noDefault()
                .name(Constants.COLUMN_MODEL_TAG).type().stringType().noDefault()
                .name(Constants.COLUMN_TIMESTAMP).type().stringType().noDefault()
                .endRecord();
   
    	return outputSchema;
    }
}
