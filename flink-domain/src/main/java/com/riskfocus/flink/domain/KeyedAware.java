package com.riskfocus.flink.domain;

/**
 * @author Khokhlov Pavel
 */
public interface KeyedAware {
    /**
     *
     * @return the byte[] value to be used as the Key when reading/writing to Kafka. Null if there is no key.
     */
    byte[] key();
}
