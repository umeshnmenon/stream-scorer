package com.ml.stream.kafkastream.transformers;

import java.util.ArrayList;
import java.util.Iterator;

import com.ml.stream.kafkastream.constants.Constants;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.ml.stream.kafkastream.model.Model;
import com.ml.stream.kafkastream.utils.MLUtil;

/**
 * @author Umesh.Menon
 * This is a Kafka DSL Transformer class which `filter`s the stream for A/B Testing purpose.
 * The idea is we can either pass the model id in message header or message payload. This 
 * transformer checks both header and payload if there's model_uuid or model_tag provided. 
 * If yes, it checks whether the scorer object has the same model_uuid or model_tag and if they 
 * match, the record will be processed.
 */
public class ABTestingTransformer implements Transformer<String, GenericRecord, KeyValue<String, GenericRecord>>{

	private ProcessorContext context;
	private ArrayList<Model> models;
	
	public ABTestingTransformer(ArrayList<Model> models){
		this.models = models;
	}
	
	@Override
	public void init(ProcessorContext context) {
	    this.context = context;
	}
	
	 @Override
	 public KeyValue<String, GenericRecord> transform(String key, GenericRecord value) {
	    // transform key and/or value or use context.forward to produce multiple messages
	    //return new KeyValue<>(key, value);
		// first check the headers
		 Headers headers = this.context.headers();
		 Iterator<Header> iterator = headers.iterator();
		 String model_uuid;
		 String model_tag;
		 Boolean canMove = true;
		 while (iterator.hasNext()) {
				Header next = iterator.next();
				// check for model uuid
				if (next.key().equals(Constants.MODEL_UUID)) {
					model_uuid = next.value().toString();
					if ((!model_uuid.equals("")) && model_uuid != null){
						// check if the model id provided is in the models served list
						if (!MLUtil.ifModelIdMatches(models, model_uuid)) canMove = false;
						break;
					}
				}
				// check for model tag
				if (next.key().equals(Constants.MODEL_TAG)) {
					model_tag = next.value().toString();
					if ((!model_tag.equals("")) && model_tag != null){
						// check if the model id provided is in the models served list
						if (!MLUtil.ifModelTagMatches(models, model_tag)) canMove = false;
						break;
					}
				}
		}
		 
		// If the model data is not provided in the header then check the payload
		 if (canMove){
			if (MLUtil.ifModelIdExistsInPayload(value)){
				model_uuid = value.get(Constants.MODEL_UUID).toString();
				if (!MLUtil.ifModelIdMatches(models, model_uuid)) canMove = false;	
			}
			
			if (MLUtil.ifModelTagExistsInPayload(value)){
				model_tag = value.get(Constants.MODEL_TAG).toString();
				if (!MLUtil.ifModelTagMatches(models, model_tag)) canMove = false;
			}
		 }
		
		 // if canMove true forward or else do nothing
		 if (canMove){
			 context.forward(key, value);
		 }
		 return null;
		 
		 // if the above doesn't work add new column to the record and filter later based on that
		 //value.put(Constants.CAN_FORWARD, canMove.toString());
		 //return new KeyValue<>(key, value);
	 }
	 
	 /*
	  @Override
	  public KeyValue<String, GenericRecord> punctuate(long timestamp) {
	    context.forward(key, value);
	    return null; // method should always return null
	  }
	  */
	  @Override
	  public void close() {  }
}
