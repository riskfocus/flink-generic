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
