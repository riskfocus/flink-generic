package com.riskfocus.flink.snapshot.context;

import lombok.*;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Getter
@Setter
@ToString
public class ContextMetadata implements Serializable {
    private static final long serialVersionUID = 3175629303519227784L;

    /**
     * Context identifier (has to be provided by ContextService)
     */
    private long id;
    /**
     * Date of snapshot in format "yyyyMMdd" (has to be provided by ContextService)
     */
    private String date;

    /**
     * Name of provided Context
     */
    private String contextName;
}
