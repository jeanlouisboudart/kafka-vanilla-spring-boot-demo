package com.example.demo.config;

import com.example.demo.kafka.ErrorType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class KafkaErrorHandlerMetrics {

    /**
     * Metrics
     */
    private static final String SKIPPED_RECORDS_METRICS = "kafka-error-skipped-records";
    private static final String FATAL_ERROR_METRICS = "kafka-error-fatal-records";
    /**
     * TAGS
     */
    private static final String TAG_ERROR_TYPE = "error-type";
    private static final String TAG_EXCEPTION_CLASS = "exception-class";
    public static final String DETAIL_SUFFIX = "-detail";

    private final MeterRegistry meterRegistry;


    public Counter totalSkippedRecords() {
        return meterRegistry.counter(SKIPPED_RECORDS_METRICS);
    }


    public Counter totalSkippedRecords(ErrorType errorType, Exception exception) {
        return meterRegistry.counter(SKIPPED_RECORDS_METRICS + DETAIL_SUFFIX,
                TAG_ERROR_TYPE, errorType.name(),
                TAG_EXCEPTION_CLASS, exception.getClass().getCanonicalName()
        );
    }

    public Counter totalFatalError() {
        return Counter.builder(FATAL_ERROR_METRICS).register(meterRegistry);
    }


    public Counter totalFatalError(ErrorType errorType, Exception exception) {
        return meterRegistry.counter(FATAL_ERROR_METRICS + DETAIL_SUFFIX,
                TAG_ERROR_TYPE, errorType.name(),
                TAG_EXCEPTION_CLASS, exception.getClass().getCanonicalName()
        );
    }

}
