package com.ml.stream.kafkastream.transformers;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

/**
 * @author Umesh.Menon
 *	This transformer adds timestamp to all output records. This is a supplier class to the TimestampTransformer. 
 */
public class TimestampTransformerSupplier implements TransformerSupplier<String, GenericRecord, KeyValue<String, GenericRecord>> {
	@Override 
	public Transformer<String, GenericRecord, KeyValue<String, GenericRecord>> get() {
		return new TimestampTransformer();
	}

}
