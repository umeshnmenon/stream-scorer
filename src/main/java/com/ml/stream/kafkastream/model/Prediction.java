package com.ml.stream.kafkastream.model;

/**
 * @author Umesh.Menon
 * All models must produce the prediction as Prediction object so that Scorer can easily use them
 */
public class Prediction {
	private Double value;
	private String label;
	private double[] probabilities;
	private double prob0;
	private double prob1;
	private Object base;

	public double[] getProbabilities() {
		return probabilities;
	}

	public void setProbabilities(double[] classProbabilities) {
		this.probabilities = classProbabilities;
	}

	public double getProb0() {
		return prob0;
	}

	public void setProb0(double prob0) {
		this.prob0 = prob0;
	}

	public double getProb1() {
		return prob1;
	}

	public void setProb1(double prob1) {
		this.prob1 = prob1;
	}

	public Object getBase() {
		return base;
	}

	public void setBase(Object base) {
		this.base = base;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public Double getValue() {
		return value;
	}
	
	public void setLabel(String value) {
		this.label = value;
	}

	public String getLabel() {
		return label;
	}
}
