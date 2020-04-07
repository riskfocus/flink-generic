package com.riskfocus.flink.example.snapshot;

import com.riskfocus.flink.example.domain.SimpleCurrency;
import com.riskfocus.flink.snapshot.context.Context;
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


    Optional<SnapshotData<SimpleCurrency>> loadSimpleCurrency(Context context, String code) throws IOException;

}