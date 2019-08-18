package com.ml.stream.kafkastream.model;


/**
 * @author Umesh.Menon
 * An abstract class that all the models must implements. This will be used as the type in Scorer class.
 */
public abstract class Model {
	protected String modelUUID;
	protected String modelTag;
	protected String modelClassName;
	protected Object model;
	
	public Model(String modelUUID, String modelTag, String modelClassName){
		// we will enforce the subclasses to set the model id and tag so that we will know
		// which model has done the prediction
		this.modelUUID = modelUUID;
		this.modelTag = modelTag;
		this.modelClassName = modelClassName;
		loadModel();
	}
	
	public abstract void loadModel();
	
	//public abstract Object predict(GenericRecord record);
	public abstract Prediction predict(RowData rowData);
	
	//public abstract Object getPredictionOutput();
	
	public String getModelUUID(){
		return modelUUID;
	}
	
	public String getModelTag(){
		return modelTag;
	}
	
	public String getModelClassName(){
		return modelClassName;
	}
}
