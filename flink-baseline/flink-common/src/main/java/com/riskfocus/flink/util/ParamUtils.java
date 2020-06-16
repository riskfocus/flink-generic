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
        String paramValue = retrieveParam(parameterName);
        if (paramValue == null) {
            return defaultValue;
        }
        return paramValue;
    }

    public int getInt(String parameterName, int defaultValue) {
        String paramValue = retrieveParam(parameterName);
        if (StringUtils.isBlank(paramValue)) {
            return defaultValue;
        }
        return Integer.parseInt(paramValue);
    }

    public long getLong(String parameterName, long defaultValue) {
        String paramValue = retrieveParam(parameterName);
        if (StringUtils.isBlank(paramValue)) {
            return defaultValue;
        }
        return Long.parseLong(paramValue);
    }

    public boolean getBoolean(String parameterName, boolean defaultValue) {
        String paramValue = retrieveParam(parameterName);
        if (StringUtils.isBlank(paramValue)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(paramValue);
    }

    /**
     *
     * @param parameterName name of parameter
     * @return true if parameter has value
     */
    public boolean has(String parameterName) {
        String paramValue = retrieveParam(parameterName);
        return !StringUtils.isBlank(paramValue);
    }

    private String retrieveParam(String parameterName) {
        String paramValue = params.get(parameterName);
        String envKey = buildEnvKey(parameterName);
        String envValue = getEnv().get(envKey);
        if (paramValue == null) {
            if (envValue != null) {
                return envValue;
            }
        }
        return paramValue;
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
