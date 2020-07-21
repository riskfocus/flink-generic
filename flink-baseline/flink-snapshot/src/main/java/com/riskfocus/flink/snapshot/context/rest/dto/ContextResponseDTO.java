/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.snapshot.context.rest.dto;

import com.google.common.annotations.Beta;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Beta
public class ContextResponseDTO implements Serializable {
    private static final long serialVersionUID = -8237718574716746740L;
    private long ctxId;
}
