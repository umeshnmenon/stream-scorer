package com.ml.stream.kafkastream.utils;

import java.util.ArrayList;

import com.ml.stream.kafkastream.constants.Constants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.ml.stream.kafkastream.model.Model;

/**
 * @author Umesh.Menon
 * This class contains all the machine learning related utility functions
 */
public class MLUtil {

	/**
	   * Checks if a model uuid exists in the model list
	   * @param models
	   * @param model_uuid
	   * @return
	   */
	  public static Boolean ifModelIdMatches(ArrayList<Model> models, String model_uuid){
		  for (Model model: models){
			  if (model.getModelUUID().equals(model_uuid)){
				  return true;
			  }
		  }
		  return false;
	  }
	  
	  /**
	   * Checks if a model uuid exists in the model list
	   * @param models
	   * @param model_tag
	   * @return
	   */
	  public static Boolean ifModelTagMatches(ArrayList<Model> models, String model_tag){
		  for (Model model: models){
			  if (model.getModelTag().equals(model_tag)){
				  return true;
			  }
		  }
		  return false;
	 }
	  
	  public static Boolean ifModelIdExistsInHeader(){
		  return false;
	  }
	  
	  public static Boolean ifModelTagExistsInHeader(){
		  return false;
	  }
	  
	  /**
	   * Checks if model uuid exists in the message payload
	   * @param value
	   * @return
	   */
	  public static Boolean ifModelIdExistsInPayload(GenericRecord value){
		  Schema schema = value.getSchema();
		  for (Schema.Field field : schema.getFields()){
			  if (field.name().equals(Constants.MODEL_UUID)) return true;
	      }
		  return false;
	  }
	  
	  /**
	   * Checks if model tag exists in the message payload
	   * @param record
	   * @return
	   */
	  public static Boolean ifModelTagExistsInPayload(GenericRecord record){
		  Schema schema = record.getSchema();
		  for (Schema.Field field : schema.getFields()){
			  if (field.name().equals(Constants.MODEL_TAG)) return true;
	      }
		  return false;
	  }
	 
}
