package com.ml.stream.kafkastream.exception.handlers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 * This class contains the exception handler for Deserialization
 */
public class DeserializationExceptionHandler extends LogAndContinueExceptionHandler{
    private static Logger log = LoggerFactory.getLogger(DeserializationExceptionHandler.class.getSimpleName());
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {

        log.error("Exception caught during Deserialization, " +
                        "taskId: {}, topic: {}, partition: {}, offset: {}",
                context.taskId(), record.topic(), record.partition(), record.offset(),
                exception);
        return super.handle(context, record, exception);
    }
}
