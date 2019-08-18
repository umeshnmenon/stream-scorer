package com.ml.stream.kafkastream.model;

/**
 * @author Umesh.Menon
 * Contains the type of Prediction value. Currently only four types are supported.
 * 1. VALUE (Regression prediction output)
 * 2. LABEL (Classification predicted label)
 * 3. PROB1 (Classification predicted probabilities of positive class)
 * 4. PROB0 (Classification predicted probabilities of negative class)
 */
public enum PredictValueType {
	VALUE,
	LABEL,
	PROB1,
	PROB0
}
