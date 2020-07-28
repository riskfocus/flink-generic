/*
 * Copyright 2020 Risk Focus Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
