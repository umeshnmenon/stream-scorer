package com.ml.stream.kafkastream.record;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ml.stream.kafkastream.utils.AvroUtil;

/**
 * @author Umesh.Menon
 * This class helps build specific record according to an input Kafka topic from a generic record read by
 * the stream or consumer
 */
public class InputRecord {
	
	private static Logger logger = LoggerFactory.getLogger(InputRecord.class.getSimpleName());
	
	/**
	 * Builds a specific record from a given Generic record read from the stream
	 * @param value
	 * @return
	 */
	public static GenericRecord build(GenericRecord value, Schema schema){
		GenericRecord inputRecord = null;
		logger.info("Creating input record....");
		try {
			inputRecord = (GenericRecord) AvroUtil.genericToSpecific(value, schema);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("Error while converting GenericRecord to SpecificRecord. Error: " + e.getMessage());
			e.printStackTrace();
		}
		return inputRecord;
	}
}
