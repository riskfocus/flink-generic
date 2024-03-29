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

package com.ness.flink.snapshot.context.rest.dto;

import com.google.common.annotations.Beta;
import lombok.*;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Beta
public class ContextRequestDTO implements Serializable {

    private static final long serialVersionUID = 1684955987856874019L;
    private long windowId;
    private String dateStr;
    private String contextName;
}
