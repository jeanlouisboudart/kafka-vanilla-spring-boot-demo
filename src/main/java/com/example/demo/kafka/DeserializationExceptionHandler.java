package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;

public interface DeserializationExceptionHandler {
    DeserializationHandlerResponse handle(final ConsumerRecord<byte[], byte[]> record,
                                          final Exception exception);


    enum DeserializationHandlerResponse {
        /* continue with processing */
        CONTINUE(0, "CONTINUE"),
        /* fail the processing and stop */
        FAIL(1, "FAIL");

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
