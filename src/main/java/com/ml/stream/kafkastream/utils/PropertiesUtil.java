package com.ml.stream.kafkastream.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 
 * @author Umesh.Menon
 * This class contains necessary functions to use in Properties
 */
public final class PropertiesUtil {
	private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class.getSimpleName());
    public static final PropertiesUtil instance = new PropertiesUtil();
    //private static PropertiesUtil instance;
    private static Properties props;
    private static final String FILE_SEPARATOR = "/";

    private PropertiesUtil() {
        // add code here
    }

    /*
    public static PropertiesUtil getInstance(String resourceName){
        if(instance == null){
            instance = new PropertiesUtil(resourceName);
        }
        return instance;
    }
    */
    
    /**
     * Gets value from Property
     * @param key
     * @return
     */
    public static String getValue(String key) {
        return props.getProperty(key);
    }
    
    public static Properties getAllProperties(){return props;}

    public static Properties getProps() {
        return props;
    }

    public String getString(String key){
       return (String) props.get(key);
    }

    /**
     * Loads the property file with a given resourceName
     * @param resourceName
     * @return
     */
    public static Properties load(String resourceName){
    	
        String jarPath = PropertiesUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        int lastSlashIndex = jarPath.lastIndexOf(FILE_SEPARATOR);
        String parentFolder = jarPath.substring(0, lastSlashIndex);
        String propertiesLocation = parentFolder + FILE_SEPARATOR + resourceName + ".properties";
        logger.info("Loaded resource file: {}", propertiesLocation);
        props = _load(propertiesLocation);
        /*
        // This will not work from a jar
        // alternate way
        URL res = PropertiesUtil.class.getClassLoader().getResource(resourceName + ".properties");
        File resFile = null;
		try {
			logger.info("Loaded resource file: {}", res.toURI());
			resFile = Paths.get(res.toURI()).toFile();
		} catch (URISyntaxException e1) {
			logger.error("Error while reading the resource file. Error: {}", e1.getMessage());
			e1.printStackTrace();
		}
        propertiesLocation = resFile.getAbsolutePath();
        props = _load(propertiesLocation);
        */
        // Loading as stream
        /*
    	props = loadFromStream(resourceName + ".properties");
        */
    	return props;
    }
    
    /**
     * Loads a property from file location
     * @param resourceFile
     * @return
     */
    public static Properties loadFromFile(String resourceFile){
        return _load(resourceFile);
    }
    
    /**
     * Loads a property from file location
     * @param resourceFile
     * @return
     */
    private static Properties _load(String resourceFile){
    	logger.info("Loading properties from " + resourceFile + "...");
    	props = new Properties();
        try (FileInputStream file = new FileInputStream(resourceFile)) {
            props.load(file);
            logger.debug("Loaded properties {}", props);
        } catch (IOException e) {
            logger.error("Error in loading properties :{}", e);
        }
        return props;
    }
    
    /**
     * Loading the resource file as stream
     * https://stackoverflow.com/questions/20389255/reading-a-resource-file-from-within-jar
     * @param resourceName
     * @return
     */
    private static Properties loadFromStream(String resourceName){
    	logger.info("Loading properties from " + resourceName + "...");
    	props = new Properties();
        try (
        		//InputStream in = PropertiesUtil.class.getResourceAsStream(resourceName); 
        		InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(resourceName); 
        		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        	) {
            props.load(reader);
            logger.debug("Loaded properties {}", props);
        } catch (IOException e) {
            logger.error("Error in loading properties :{}", e);
        }
        return props;
    }
}
