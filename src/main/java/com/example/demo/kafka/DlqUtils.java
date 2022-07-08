package com.example.demo.kafka;

import org.apache.kafka.common.header.Headers;

public class DlqUtils {
    public static final String DLQ_HEADER_PREFIX = "dlq.error.";

    public static final String DLQ_HEADER_APP_NAME = DLQ_HEADER_PREFIX + "app.name";
    public static final String DLQ_HEADER_TIMESTAMP = DLQ_HEADER_PREFIX + "timestamp";
    public static final String DLQ_HEADER_TOPIC = DLQ_HEADER_PREFIX + "topic";
    public static final String DLQ_HEADER_PARTITION = DLQ_HEADER_PREFIX + "partition";
    public static final String DLQ_HEADER_OFFSET = DLQ_HEADER_PREFIX + "offset";
    public static final String DLQ_HEADER_EXCEPTION_CLASS = DLQ_HEADER_PREFIX + "exception.class.name";
    public static final String DLQ_HEADER_EXCEPTION_MESSAGE = DLQ_HEADER_PREFIX + "exception.message";

    public static void addHeader(Headers headers, String headerName, String value) {
        byte[] valueAsBytes = value != null ? value.getBytes() : null;
        addHeader(headers, headerName, valueAsBytes);
    }

    public static void addHeader(Headers headers, String headerName, Number value) {
        byte[] valueAsBytes = value != null ? value.toString().getBytes() : null;
        addHeader(headers, headerName, valueAsBytes);
    }

    public static void addHeader(Headers headers, String headerName, byte[] value) {
        removeHeader(headers, headerName);
        headers.add(headerName, value);

    }

    public static void removeHeader(Headers headers, String headerName) {
        headers.remove(headerName);
    }

    public static String getHeaderAsString(Headers headers, String headerName) {
        return new String(headers.lastHeader(headerName).value());
    }

    public static Integer getHeaderAsInt(Headers headers, String headerName) {
        return Integer.parseInt(getHeaderAsString(headers, headerName));
    }

}
