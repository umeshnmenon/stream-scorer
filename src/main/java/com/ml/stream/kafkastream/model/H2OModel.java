package com.ml.stream.kafkastream.model;

import com.ml.stream.kafkastream.utils.H2OUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hex.genmodel.GenModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import hex.genmodel.easy.prediction.RegressionModelPrediction;

/**
 * @author Umesh.Menon
 * This class is a wrapper for H2O model and exposes functions to convert a H20 prediction object to
 * our own Prediction object.
 */
public class H2OModel extends Model{
	private static Logger logger = LoggerFactory.getLogger(H2OModel.class.getSimpleName());
	private static EasyPredictModelWrapper model; //overriding the model type 
	
	public H2OModel(String modelUUID, String modelTag, String modelClassName){
		// we will enforce the subclasses to set the model id and tag so that we will know
		// which model has done the prediction
		super(modelUUID, modelTag, modelClassName);
	}
	
	@Override
	public void loadModel(){
		try {
			logger.info("Loading the model:" + modelClassName );
            GenModel rawModel = (GenModel) Class.forName(modelClassName).newInstance();
            model = new EasyPredictModelWrapper(rawModel);
        } catch (Exception e) {
            logger.error("Error in creating PredictiveModel :", e);
        }
	}
	
	@Override
	public Prediction predict(com.ml.stream.kafkastream.model.RowData record){
		// Convert the GenericRecord to a RowData
		RowData rowData = H2OUtil.castToRowData(record);
		//return model.predict(rowData, model.getModelCategory())[0];
		Object prediction = null;
		// checks only regression and classification for now
		try {
			switch (model.getModelCategory()){
		        case Regression:
					prediction = model.predictRegression(rowData);
		            break;
		        case Binomial:
		            prediction = model.predictBinomial(rowData);
		            break;
		        case Multinomial:
		        	prediction = model.predictMultinomial(rowData);
				default:
					break;
		    }
		} catch (PredictException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error("Error while predicting. Error: " + e.getMessage());
		}
		// prepare the prediction object
		Prediction pred = this.preparePrediction(prediction);
		return pred;
	}
	
	/**
	 * Creates a prediction object
	 * @param prediction
	 * @param value
	 * @param probabilities
	 * @param prob0
	 * @param prob1
	 * @return
	 */
	public Prediction preparePrediction(Object prediction){
		Prediction pred = new Prediction();
		pred.setBase(prediction);
		switch (model.getModelCategory()){
		    case Regression:
		    	RegressionModelPrediction regPred = (RegressionModelPrediction) prediction;
		    	pred.setValue(regPred.value);
		        break;
		    case Binomial:
		    	BinomialModelPrediction binPred = (BinomialModelPrediction) prediction;
		    	pred.setLabel(binPred.label);
		    	pred.setProbabilities(binPred.classProbabilities);
		    	pred.setProb0(binPred.classProbabilities[0]);
		    	pred.setProb1(binPred.classProbabilities[1]);
		    	break;
		default:
			break;
		}
		return pred;
	}
	
	/**
	 * OBSOLETE. Get the prediction output from Prediction object
	 * @param pred
	 * @return
	 */
	public Object getPredictionOutput(Prediction pred) throws PredictException{
		Object output = null;
		switch (model.getModelCategory()){
		    case Regression:
				output = pred.getValue();			
		        break;
		    case Binomial:
		    	output = pred.getProb1();
		        break;
		    case Multinomial:
		    	output = pred.getProb1();
		default:
			break;
		}
		return output;
	}

}
