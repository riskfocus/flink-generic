package com.riskfocus.flink.util;

import com.google.common.base.CaseFormat;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public final class ParamUtils implements Serializable {
    private static final long serialVersionUID = -5742005473009969633L;

    private final ParameterTool params;

    /**
     * Get parameter for Flink based application
     * Priority is: command argument, environment variable, default value
     *
     *
     * @param parameterName name of argument parameter
     * @param defaultValue default value
     * @return value of parameter
     */
    public String getString(String parameterName, String defaultValue) {
        String envKey = buildEnvKey(parameterName);
        String envValue = getEnv().get(envKey);
        if (StringUtils.isBlank(envValue)) {
            return params.get(parameterName, defaultValue);
        } else {
            return envValue;
        }
    }

    public int getInt(String parameterName, int defaultValue) {
        String envKey = buildEnvKey(parameterName);
        String envValue = getEnv().get(envKey);
        if (StringUtils.isBlank(envValue)) {
            return params.getInt(parameterName, defaultValue);
        } else {
            return Integer.parseInt(envValue);
        }
    }

    public long getLong(String parameterName, long defaultValue) {
        String envKey = buildEnvKey(parameterName);
        String envValue = getEnv().get(envKey);
        if (StringUtils.isBlank(envValue)) {
            return params.getLong(parameterName, defaultValue);
        } else {
            return Long.parseLong(envValue);
        }
    }

    public boolean getBoolean(String parameterName, boolean defaultValue) {
        String envKey = buildEnvKey(parameterName);
        String envValue = getEnv().get(envKey);
        if (StringUtils.isBlank(envValue)) {
            return params.getBoolean(parameterName, defaultValue);
        } else {
            return Boolean.parseBoolean(envValue);
        }
    }

    /**
     *
     * @param parameterName name of parameter
     * @return true if parameter has value
     */
    public boolean has(String parameterName) {
        String envKey = buildEnvKey(parameterName);
        String envValue = getEnv().get(envKey);
        if (StringUtils.isBlank(envValue)) {
            return params.has(parameterName);
        } else {
            return true;
        }
    }

    private String buildEnvKey(String parameterName) {
        String replaced = parameterName.replace(".", "-");
        return CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_UNDERSCORE, replaced);
    }

    @SuppressWarnings("java:S5304")
    private Map<String, String> getEnv() {
        return System.getenv();
    }

    public ParameterTool getParams() {
        return params;
    }
}
