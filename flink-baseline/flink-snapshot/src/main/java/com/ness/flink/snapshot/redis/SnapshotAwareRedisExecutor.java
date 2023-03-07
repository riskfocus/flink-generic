//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.snapshot.redis;

import com.ness.flink.domain.TimeAware;
import com.ness.flink.snapshot.context.ContextService;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.NonNull;
import org.apache.flink.api.common.functions.Function;

import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface SnapshotAwareRedisExecutor<T extends TimeAware> extends Function {
    void execute(@NonNull T element, @NonNull ContextService contextService, @NonNull String contextName,
                 @NonNull StatefulRedisConnection<byte[], byte[]> connection) throws IOException;
}
