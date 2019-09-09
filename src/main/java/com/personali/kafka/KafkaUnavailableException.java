package com.personali.kafka;

/**
 * Created by orsher on 5/8/18.
 */
public class KafkaUnavailableException extends Exception {
    public KafkaUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
