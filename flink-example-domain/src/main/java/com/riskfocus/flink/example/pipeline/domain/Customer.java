package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Customer extends IncomingEvent {
    private static final long serialVersionUID = -72956043719674371L;

    private int customerId;
    private String name;
    private String email;

}