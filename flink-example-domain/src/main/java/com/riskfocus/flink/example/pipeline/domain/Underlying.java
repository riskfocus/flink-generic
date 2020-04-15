package com.riskfocus.flink.example.pipeline.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Underlying implements Serializable {
    private static final long serialVersionUID = 8154148175896411167L;

    private String name;

    public static Underlying of(String name) {
        return new Underlying(name);
    }
}