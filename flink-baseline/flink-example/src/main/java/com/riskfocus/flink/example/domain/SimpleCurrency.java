package com.riskfocus.flink.example.domain;

import com.riskfocus.flink.domain.TimeAware;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class SimpleCurrency implements TimeAware {
    private long timestamp;
    private String code;
    private double rate;
}
