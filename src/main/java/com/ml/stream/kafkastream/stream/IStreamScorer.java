package com.ml.stream.kafkastream.stream;

import com.ml.stream.kafkastream.model.Prediction;
import com.ml.stream.kafkastream.model.RowData;

/**
 * @author Umesh.Menon
 *  Interface for ML Scorer Processors. So any similar scorer/processor must implement the below
 *  methods.
 */
public interface IStreamScorer {
    //public Object score(HashMap<String, Object> rowData);
	public Prediction score(RowData rowData);
}
