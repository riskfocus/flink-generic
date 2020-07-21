/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.snapshot;

import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.redis.SnapshotData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
public interface InterestRatesLoader extends Serializable, AutoCloseable {

    /**
     * Here you can implement any initialization steps
     * (open connection to Redis etc)
     * @throws IOException
     */
    void init() throws IOException;

    /**
     * Get InterestRates by Context
     * @param context context
     * @return InterestRates
     * @throws IOException
     */
    Optional<SnapshotData<InterestRates>> loadInterestRates(ContextMetadata context) throws IOException;
}
