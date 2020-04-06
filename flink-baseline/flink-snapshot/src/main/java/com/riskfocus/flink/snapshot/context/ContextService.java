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
     * @param contextName name of provided context
     * @return context ready for use
     */
    Context generate(TimeAware timeAware, String contextName);

    /**
     * Service initialization
     */
    void init();

}
