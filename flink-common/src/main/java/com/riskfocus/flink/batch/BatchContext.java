package com.riskfocus.flink.batch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Context Service should provide BatchContext
 *
 * @author Khokhlov Pavel
 */
@ToString
@AllArgsConstructor
@Getter
public class BatchContext implements Serializable {

    private static final long serialVersionUID = -8423785994398814432L;

    private final long id;
    private final long start;
    private final long end;

    /**
     *
     * @return the end time for current batch
     */
    public long endOfBatch() {
        return end - 1;
    }

    public long duration() {
        return end - start;
    }
}

