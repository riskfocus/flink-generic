package com.riskfocus.flink.snapshot.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Getter
public class SnapshotData<T> {
    private final long contextId;
    private final T element;
}