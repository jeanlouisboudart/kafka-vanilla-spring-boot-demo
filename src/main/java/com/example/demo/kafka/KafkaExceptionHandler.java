package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaExceptionHandler {

    <K, V> DeserializationHandlerResponse handleProcessingError(final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);

    <K, V> DeserializationHandlerResponse handleDeserializationError(final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record);


    enum DeserializationHandlerResponse {
        /* continue with processing */
        VALID(0, "VALID"),
        /* continue with processing */
        IGNORE(1, "IGNORE"),
        /* fail the processing and stop */
        FAIL(-1, "FAIL");

        /**
         * an english description of the api--this is for debugging and can change
         */
        public final String name;

        /**
         * the permanent and immutable id of an API--this can't change ever
         */
        public final int id;

        DeserializationHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
