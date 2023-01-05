/*
 * Copyright 2020-2023 Ness USA, Inc.
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

package com.ness.flink.config.channel.kafka;

import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import java.io.Serializable;
import lombok.experimental.SuperBuilder;

/**
 * @author Khokhlov Pavel
 */
@SuperBuilder
public abstract class KafkaValueAwareSink<S extends Serializable> extends KafkaAwareSink<S> {
    protected final Class<S> domainClass;
    protected final KeyExtractor<S> keyExtractor;
    protected final EventTimeExtractor<S> eventTimeExtractor;

}
