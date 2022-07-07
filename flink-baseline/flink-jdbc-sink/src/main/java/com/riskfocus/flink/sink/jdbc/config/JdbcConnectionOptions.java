/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.riskfocus.flink.sink.jdbc.config;

import lombok.*;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(setterPrefix = "with")
@ToString
public class JdbcConnectionOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private String dbURL;
    private String driverName;
    private String username;
    private String password;
    private Boolean autoCommit;
    private boolean useDbURL;

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public Optional<Boolean> getAutoCommit() {
        return Optional.ofNullable(autoCommit);
    }

}
