package com.ml.stream.kafkastream.transformers;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

import com.ml.stream.kafkastream.utils.CoreUtils;

/**
 * @author Umesh.Menon
 * This class adds a timestamp to the record. So any records in the stream can use this transformer to 
 * add timestamp to it.
 */
public class TimestampTransformer implements Transformer<String, GenericRecord, KeyValue<String, GenericRecord>>{

	private ProcessorContext context;

	@Override
	public void init(ProcessorContext context) {
	    this.context = context;
	}
	
	public KeyValue<String, GenericRecord> transform(String key, GenericRecord value) {
        long ts = CoreUtils.getCurrentTimestamp();
		// you can call #forward() as often as you want
        context.forward(key, value, To.all().withTimestamp(ts));

        return null; // only return data via context#forward()
      }

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
