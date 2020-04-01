package com.riskfocus.flink.snapshot.redis;

import com.google.common.annotations.Beta;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@Beta
@AllArgsConstructor
@Getter
public class ContextHolder<T> {
    private final long contextId;
    private final T element;
}