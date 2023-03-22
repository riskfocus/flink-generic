/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.sink.jdbc.core.output;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public abstract class AbstractSinkWriter<T> implements SinkWriter<T>, Serializable {
    private static final long serialVersionUID = 1L;

    protected final String sinkName;
    protected transient InitContext context;

    protected AbstractSinkWriter(String sinkName) {
        this.sinkName = checkNotNull(sinkName);
    }

    @SneakyThrows
    public void open(InitContext context) {
        this.context = context;
    }

}
