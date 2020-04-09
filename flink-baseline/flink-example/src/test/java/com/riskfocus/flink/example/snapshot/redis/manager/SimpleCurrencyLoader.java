package com.riskfocus.flink.example.snapshot.redis.manager;

import com.riskfocus.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.redis.SnapshotData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
public interface SimpleCurrencyLoader extends Serializable, AutoCloseable {

    /**
     * Here you can implement any initialization steps
     * (open connection to Database/Redis etc)
     *
     * @throws IOException
     */
    void init() throws IOException;


    Optional<SnapshotData<SimpleCurrency>> loadSimpleCurrency(ContextMetadata contextMetadata, String code) throws IOException;

}