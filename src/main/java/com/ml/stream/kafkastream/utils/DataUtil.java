package com.ml.stream.kafkastream.utils;

import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

import com.ml.stream.kafkastream.model.RowData;

/**
 * @author Umesh.Menon
 * This class contains all the data operation utility functions 
 */
public class DataUtil {

	/**
    * Create a RowData from a GenericRecord and Feature set. Only features that are there in the 
    * feature set will be picked from the GenericRecord and will be converted to a simple HashMap
    * that's easy to use.
    * @param features
    * @return
    */
   public static RowData populateRow(GenericRecord record, Map<String, String> features){
       // TODO: Make it to load different data types and use generic for H20 RowData
       RowData row = new RowData();
       for (Map.Entry<String, String> entry : features.entrySet()) {
           row.put(entry.getValue(), (record.get(entry.getKey())).toString());
       }
       return row;
   }
   
   /**
    * Create a RowData from a GenericRecord. All the fields in the GenericRecord will be added to the
    * RowData.
    * @param value
    * @param features
    * @return
    */
   public static RowData populateRow(GenericRecord record){
       // TODO: Make it to load different data types and use generic for H20 RowData
       RowData row = new RowData();
       for (Field field : record.getSchema().getFields()) {
           row.put(field.name(), (record.get(field.name())).toString());
       }
       return row;
   }
}
