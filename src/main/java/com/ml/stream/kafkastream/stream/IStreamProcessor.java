package com.ml.stream.kafkastream.stream;

import org.apache.avro.generic.GenericRecord;

/**
 * @author Umesh.Menon
 * Any processor to be used in StreamScorerService must implement this interface
 */
public interface IStreamProcessor {
    public GenericRecord run(GenericRecord value);
}
