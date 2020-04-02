package com.riskfocus.flink.snapshot.context;

import com.riskfocus.flink.domain.TimeAware;

/**
 * @author Khokhlov Pavel
 */
public interface ContextService extends AutoCloseable {

    /**
     * See create method
     * https://wiki.riskfocus.com/display/OCC/Context+Service
     * @param timeAware element which can provide timestamp
     * @return
     */
    Context generate(TimeAware timeAware);

    /**
     * Service initialization
     */
    void init();

}
