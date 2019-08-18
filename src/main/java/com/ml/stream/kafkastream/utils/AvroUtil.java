package com.ml.stream.kafkastream.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.io.ByteArrayOutputStream;
import java.io.IOException;



import com.jayway.jsonpath.JsonPath;

/**
 * 
 * @author Umesh.Menon
 * This class contains all the Avro related utility functions
 */
public class AvroUtil {
	private static Logger logger = LoggerFactory.getLogger(AvroUtil.class.getSimpleName());
	
    /**
     * Converts json to an avro object
     * @param <T> type of avro record
     * @param value RowData
     * @param className   Classname of the avro record
     * @param schema      Avro schema for output
     * @return T
     * @throws IOException
     */
    //@param value Json input string
    //public <T extends GenericRecord> T jsonToAvro(String inputString, Class<T> className, Schema schema) throws IOException {
    public static <T extends GenericRecord> T  jsonToAvro(GenericRecord value, Class<T> className, Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(value.getSchema(), out);
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(className);
        //T returnObject = writer.write(new GenericRecordBuilder(schema).build(), jsonEncoder);
        //GenericRecord returnObject = writer.write(value, jsonEncoder);
        String inputString = "";
        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, inputString);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(className);
        T returnObject = reader.read(null, jsonDecoder);
        return returnObject;
    }

    /**
     * Converts a generic record to a specific record of given schema
     * @param record
     * @param className
     * @param schema
     * @return
     */
    @SuppressWarnings("finally")
	public static Object genericToSpecific(GenericRecord record, Class<?> className, Schema schema){
    	logger.info("Converting the record to specific schema....");
        logger.debug("Specific Record schema class name: " + className);
    	//Object message =  SpecificData.get().deepCopy(schema, record);
    			
    	Schema schema1 = record.getSchema(); //see if we need to pass the schema or not
    	
    	Object  specificRecord = null;
        //GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(MyCustomRecord.getClassSchema());
        //GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema1);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write((GenericRecord) record, encoder);
        	/*
        	//writer.write(ByteBuffer.wrap(record), encoder);
        	if (record instanceof byte[]){
		       writer.write(ByteBuffer.wrap((byte[])record), encoder);
		      } else {
		    	  writer.write(record, encoder);
            }
        	
        	*/
            encoder.flush();
            byte[] avroData = out.toByteArray();
            out.close();
            
            //SpecificDatumReader<?> reader = new SpecificDatumReader<>(className);
            DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema1);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);

            specificRecord = reader.read(null, decoder);
            //Object o = className.newInstance();
            return specificRecord;

        } catch (IOException e) {
        	logger.info("Error while converting to specific record schema");
            logger.error("Error in specific schema conversion: " + e.getMessage());
            //e.printStackTrace();
        } catch (Exception ex){
        	logger.info("Error while converting to specific record schema");
            logger.error("Error in specific schema conversion: " + ex.getMessage());
        	//ex.printStackTrace();
        }
        /*
        catch (AvroRuntimeException e) {
            throw new SerializationException();
        }
        catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }*/
        finally {
            return specificRecord;
        }
        // OR
        //MySpecificType message = (T SpecificData.get().deepCopy(MySpecificType.SCHEMA$, genericMessage);
        //MySpecificType message = (T SpecificData.get().deepCopy(schema, record);
    }
    
    /**
     * Converts a generic record to a specific record of given schema. Refer the above function.
     * @param record
     * @param className
     * @param schema
     * @return
     */
    @SuppressWarnings("finally")
    public static Object genericToSpecific(GenericRecord record, Schema schema){
    	logger.info("Converting the record to specific schema....");
    	//Object message =  SpecificData.get().deepCopy(schema, record);
    			
    	//Schema schema = record.getSchema(); //see if we need to pass the schema or not
    	
    	Object  specificRecord = null;
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write((GenericRecord) record, encoder);
            encoder.flush();
            byte[] avroData = out.toByteArray();
            out.close();
            
            DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);

            specificRecord = reader.read(null, decoder);
            //Object o = className.newInstance();
            return specificRecord;

        } catch (IOException e) {
        	logger.info("Error while converting to specific record schema");
            logger.error("Error in specific schema conversion: " + e.getMessage());
            //e.printStackTrace();
        } catch (Exception ex){
        	logger.info("Error while converting to specific record schema");
            logger.error("Error in specific schema conversion: " + ex.getMessage());
        	//ex.printStackTrace();
        }
        finally {
            return specificRecord;
        }
    }
    
    /**
     * Creates a generic record with a given schema from another generic record
     * @param schema
     * @param record
     * @return
     */
    public static GenericRecord createRecordFromAnother(Schema schema, GenericRecord record){
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        // TODO: Cast the values to appropriate data types mentioned in the schema
        for (Schema.Field field : schema.getFields()){
            builder.set(field, record.get(field.name()));
        }
        return builder.build();
    }
    
    /**
     * Creates a new generic record with a given schema
     * @param schema
     * @param record
     * @return
     */
    public static GenericRecord createGenericRecordWithSchema(Schema schema, HashMap<String, Object> values){
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Map.Entry mapElement : values.entrySet()) { 
            String key = (String)mapElement.getKey(); 
            Object value = mapElement.getValue();
            builder.set(key, value);
        }
        return builder.build();
    }
    
    /**
     * Adds the values to a given GenericRecord from a list
     * @param <T>
     * @param record
     * @param values
     * @return
     */
    public static <T extends HashMap<String, Object>> GenericRecord addValuesToGenericRecord(GenericRecord record, T values){
        for (Map.Entry mapElement : values.entrySet()) { 
            String key = (String)mapElement.getKey(); 
            Object value = mapElement.getValue();
            record.put(key, value);
        }
        return record;
    }
    
    /**
     * Gets the schema of the output topic
     * @param topic
     * @return
     * @throws Exception
     */
    public static Schema getAvroSchema(String topic, String schemaRegistry){
    	String subject = topic + "-value";
    	Schema schema = null;
    	String url = schemaRegistry + "/subjects/" + subject + "/versions/latest";
		try {
			logger.info("Getting avro schema from {}", url);
			schema = Optional
					.ofNullable(CoreUtils.httpGet(url,
							"application/vnd.schemaregistry.v1+json, application/vnd.", "application/json"))
					.map(item -> new Schema.Parser().parse((String) JsonPath.parse(item).read("$.schema"))).orElse(null);
		} catch (ClientProtocolException e) {
			//e.printStackTrace();
			logger.error("Error while fetching schema " + subject + " for topic " + topic + " from schema registry " + schemaRegistry + "." );
		} catch (IOException e) {
			//e.printStackTrace();
			logger.error("Error while fetching schema " + subject + " for topic " + topic + " from schema registry " + schemaRegistry + "." );
		}
		return schema;
	}
}
