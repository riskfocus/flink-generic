/*
 * Copyright 2021-2023 Ness Digital Engineering
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

package com.ness.flink.config.channel;

import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.properties.WatermarkProperties;
import java.io.Serializable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.java.utils.ParameterTool;

public interface SourceFactory {
    <S extends SpecificRecordBase> DefaultSource<S> sourceAvroSpecific(String sourceName,
        Class<S> domainClass,
        ParameterTool parameterTool,
        WatermarkProperties watermarkProperties,
        TimestampAssignerSupplier<S> timestampAssignerFunction);

    <S extends Serializable> DefaultSource<S> sourcePojo(String sourceName,
        Class<S> domainClass,
        ParameterTool parameterTool,
        WatermarkProperties watermarkProperties,
        TimestampAssignerSupplier<S> timestampAssignerFunction);
}
