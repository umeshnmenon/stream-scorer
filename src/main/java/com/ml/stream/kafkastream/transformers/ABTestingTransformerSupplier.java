package com.ml.stream.kafkastream.transformers;

import java.util.ArrayList;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

import com.ml.stream.kafkastream.model.Model;

/**
 * @author Umesh.Menon
 * This is a supplier class for ABTestingTransformer
 */
public class ABTestingTransformerSupplier implements TransformerSupplier<String, GenericRecord, KeyValue<String, GenericRecord>> {
	private ArrayList<Model> models;
	
	public ABTestingTransformerSupplier(ArrayList<Model> models){
		this.models = models;
	}
	
	@Override 
	//public Transformer<K, V, KeyValue<K, V>> get() {
	public Transformer<String, GenericRecord, KeyValue<String, GenericRecord>> get() {
		return new ABTestingTransformer(this.models);
	}
}
