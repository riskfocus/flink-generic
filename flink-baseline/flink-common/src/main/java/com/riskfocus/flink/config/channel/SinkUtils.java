/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.config.channel;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor
public final class SinkUtils<S> {

    /**
     * Build collection of Sinks
     *
     * @param sinks
     * @return
     */
    @SafeVarargs
    public final Collection<SinkInfo<S>> build(SinkMetaInfo<S>... sinks) {
        return Arrays.stream(sinks)
                .map(SinkMetaInfo::buildSink)
                .collect(Collectors.toCollection(() -> new ArrayList<>(sinks.length)));
    }
}
