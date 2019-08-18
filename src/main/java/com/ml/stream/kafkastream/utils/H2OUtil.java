package com.ml.stream.kafkastream.utils;

import com.ml.stream.kafkastream.model.RowData;

import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Umesh.Menon
 * This class contains all the H2O related utility functions
 */
public class H2OUtil {

	/**
    * Prepares the input data to be passed to an H2O model. 
    * @param features
    * @return
    */
   public static Object populateFeatueRow(GenericRecord value, Map<String, String> features){
       // TODO: Make it to load different data types and use generic for H20 RowData
	   Object row = createRowData(); 
       for (Map.Entry<String, String> entry : features.entrySet()) {
    	   ((HashMap<String, Object>) row).put(entry.getValue(), (value.get(entry.getKey())).toString());
       }
       return row;
   }
   
    /**
     * Creates a RowData using GenericRecord and Feature set. Only features that are there in the 
     * feature set will be picked from the GenericRecord and will be converted to a simple HashMap
     * that's easy to use.
     * @param features
     * @return
     */
    public static RowData populateRow(GenericRecord value, Map<String, String> features){
        // TODO: Make it to load different data types and use generic for H20 RowData
        RowData row = new RowData();
        for (Map.Entry<String, String> entry : features.entrySet()) {
            row.put(entry.getValue(), (value.get(entry.getKey())).toString());
        }
        return row;
    }

    
    /**
     * Cast the RowData to H2O gendata RowData
     * @param rowData
     * @return
     */
    public static Object castToRowData(HashMap<String, Object> rowData){
    	Object row = createRowData(); 	
		for (Map.Entry<String, Object> entry : rowData.entrySet()) {
		    ((HashMap<String, Object>) row).put(entry.getKey(), entry.getValue().toString());
		}
		return row;
    }
    
    /**
     * Create an H2O RowData. Obsolete as castToRowData() is used.
     * @return
     */
    private static Object createRowData(){
    	Object row = null;
		try {
			row = Class.forName("hex.genmodel.easy.RowData").newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//Class.getConstructor().newInstance();
		return row;
    }
    
    /**
     * Cast the RowData to H2O GenModel RowData. Obsolete as castToRowData() is used.
     * @param rowData
     * @return
     */
    public static hex.genmodel.easy.RowData castToRowData(RowData rowData){
    	hex.genmodel.easy.RowData row = new hex.genmodel.easy.RowData(); 	
		for (Map.Entry<String, Object> entry : rowData.entrySet()) {
		    row.put(entry.getKey(), entry.getValue().toString());
		}
		return row;
    }

}
