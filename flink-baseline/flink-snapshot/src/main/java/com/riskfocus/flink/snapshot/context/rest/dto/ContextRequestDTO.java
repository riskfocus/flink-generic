package com.riskfocus.flink.snapshot.context.rest.dto;

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
}
