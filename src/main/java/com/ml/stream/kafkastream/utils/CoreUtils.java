package com.ml.stream.kafkastream.utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;

import org.apache.commons.lang.ArrayUtils;

import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * 
 * @author Umesh.Menon
 * This class contains all the common utility functions
 * Created by menonu on 06/27/2019
 *
 */
public class CoreUtils {

    /**
     * Cteates a table with return values from a decreasing time function
     * @return
     */
    public static Map<String, String> jsonToMap(String json_str){
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();
        try {

            // convert JSON string to Map
            map = mapper.readValue(json_str, Map.class);

            // it works
            //Map<String, String> map = mapper.readValue(json, new TypeReference<Map<String, String>>() {});

            return map;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * Rounds a decimal value to a given precision
     * @param val
     * @param prec
     * @return
     */
    public static Double roundDec(Double val, int prec){
        double prec1 = Math.pow(10, prec);
        return (Double) (((double) Math.round(val * prec1)) / prec1);
    }

    /**
     * Casts an Object to Double
     * @param val
     * @return
     */
    public static Double getDouble(Object val) {
        Double ret = 0.0;
        if (val.getClass().getName().equals("java.lang.Long")) {
            Long lval = (Long) val;
            ret = (Double) lval.doubleValue();
        }else if (val.getClass().getName().equals("java.lang.Integer")){
            Integer iVal= (Integer) val;
            ret = (Double) iVal.doubleValue();
        }else if (val.getClass().getName().equals("java.lang.String")){
            ret = Double.parseDouble(String.valueOf(val));
        } else{
            ret = (Double) val;
        }
        return ret;
    }

    /**
     * Checks if an array contains an element
     * @param needle
     * @param haystack
     * @return
     */
    public static boolean arrayContains(String needle, String[] haystack){
        return Arrays.asList(haystack).contains(needle);
    }


    /**
     * Converts a comma separated string to an array
     * @param str
     * @return
     */
    public static String[] toArray(String str, String sep){
        String[] arr = null;
        String pattern = "\\s*".concat(sep).concat("\\s*");
        if (!str.equals("")){
            arr = str.split(pattern);
        }
        return arr;
    }

    /**
     * Converts a comma separated string to an array
     * @param str
     * @return
     */
    public static String[] toArray(String str){
        String[] arr = null;
        String pattern = "\\s*,\\s*";
        if (!str.equals("")){
            arr = str.split(pattern);
        }
        return arr;
    }

    /**
     * Converts a comma separated string of double values to a Double array
     * @param str
     * @return
     */
    public static Double[] toDoubleArray(String str){
        if (str == null || str.equals("")) return null;
        String[] arrStr = toArray(str);
        Double[] arr = new Double[arrStr.length];
        for (int i = 0; i < arrStr.length;i++){
            arr[i] = Double.parseDouble(arrStr[i]);
        }
        return arr;
    }

    /**
     * Converts a comma separated string of double values to a double array
     * @param str
     * @return
     */
    public static double[] toPrimDoubleArray(String str){
        if (str == null || str.equals("")) return null;
        String[] arrStr = toArray(str);
        double[] arr = new double[arrStr.length];
        for (int i = 0; i < arrStr.length;i++){
            arr[i] = (double) Double.parseDouble(arrStr[i]);
        }
        return arr;
    }

    /**
     * Returns the current timestamp
     * @return
     */
    public static long getCurrentTimestamp(){
        Date date = new Date();
        Timestamp ts = new Timestamp(date.getTime());
        return ts.getTime();
    }
    
    /**
     * Checks if a class exists or not
     * @param className
     * @return
     */
    public static Boolean classExists(String className){
    	if (className == null || className.equals("")){
    		return false;
    	}
		try {
			 Class.forName(className);
			} catch( ClassNotFoundException e ) {
			 return false;
		}
    	return true;
    }
    
    /**
     * Commbine two maps into one
     * @param map1
     * @param map2
     * @return
     */
    public static HashMap<String, Object> combineMaps(HashMap<String, Object> map1, HashMap<String, Object> map2){
    	HashMap<String, Object> all = new HashMap<>();
		all.putAll(map1);
		all.putAll(map2);
		return all;
    }
    
    /**
     * Converts a map to json string
     * @param map
     * @return
     */
    public static String mapToJson(Map<String, Object> map){
    	Gson gson = new Gson();
    	String jsonString = gson.toJson(map);
    	return jsonString;
    	//JsonObject json = new JsonObject();
        //json.putAll(map);
    	//return json.toString();
    }
    
    /**
     * Makes a get call
     * @param url
     * @param accept
     * @param contentType
     * @return
     * @throws ClientProtocolException
     * @throws IOException
     */
    public static String httpGet(String url, String accept, String contentType)
			throws ClientProtocolException, IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(url);
		request.setHeader(HttpHeaders.ACCEPT, accept);
		request.setHeader(HttpHeaders.CONTENT_TYPE, contentType);
		ResponseHandler<String> handler = new BasicResponseHandler();
		return client.execute(request, handler);
	}
}
