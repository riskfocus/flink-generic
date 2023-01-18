package com.ness.flink.example.snapshot.redis.manager;

import com.ness.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.redis.SnapshotData;

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