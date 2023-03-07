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

package com.ness.flink.sink.jdbc.core.output;

/**
 * Unchecked SQL Exception
 *
 * @author Khokhlov Pavel
 */
public class FailedSQLExecution extends RuntimeException {
    private static final long serialVersionUID = 5022476348745812063L;

    private static final String ERROR_MESSAGE = "Writing records to JDBC failed";

    public FailedSQLExecution(Throwable cause) {
        super(ERROR_MESSAGE, cause);
    }
}
