# Flink JDBC Sink module
This module provides JDBC Sink function.

**For now module excluded from Build, see details**: [JIRA](https://ness-nde.atlassian.net/browse/FSIP-17)

Original module has a bug which prevents of usage bulk inserts (see details: https://issues.apache.org/jira/browse/FLINK-17488)

For now module supports these [dialects](src/main/java/com/ness/flink/sink/jdbc/config/Dialect.java) 

[Job parameters configuration](src/main/java/com/ness/flink/sink/jdbc/config/JdbcSinkConfig.java)

## JDBC Sink Usage 
You have to extend your own Sink class from 
`com.ness.flink.sink.jdbc.AbstractJdbcSink`

For example:
```
public class PriceSink extends AbstractJdbcSink<PriceDto> {

    private static final long serialVersionUID = -3180581809574633032L;

    public PriceSink(JdbcSinkConfig jdbcSinkConfig) {
        super(jdbcSinkConfig);
    }

    @Override
    public String getSql() {        
        String insert = "INSERT INTO price (id, timestamp, sourceIp, sequenceNumber, messageBody) values (?, ?, ?, ?, ?)";
        String update = "timestamp = ?, sourceIp = ?, sequenceNumber = ?, messageBody = ?";
        switch (dialect) {
            case MYSQL:
                return insert + " ON DUPLICATE KEY UPDATE " + update;
            case POSTGRES:
                return insert + " ON CONFLICT (id) DO UPDATE SET " + update;
            case REDSHIFT:
                return insert;
            default:
                throw new IllegalArgumentException(String.format("Dialect %s not implemented", dialect));
        }
    }

    @Override
    public JdbcStatementBuilder<PriceDto> getStatementBuilder() {
        return (stmt, price) -> {
            try {
                final long id = price.getId();
                log.debug("Prepare statement: {}", id);
                final String sourceId = price.getSourceIp().toString();
                final String msg = price.getMessageBody().toString();
                final long sequenceNumber = price.getSequenceNumber();
                final long timestamp = price.getTimestamp();

                int idx = 0;

                stmt.setLong(++idx, id);
                stmt.setLong(++idx, timestamp);
                stmt.setString(++idx, sourceId);
                stmt.setLong(++idx, sequenceNumber);
                stmt.setString(++idx, msg);

                if (dialect != Dialect.REDSHIFT) {
                    stmt.setLong(++idx, timestamp);
                    stmt.setString(++idx, sourceId);
                    stmt.setLong(++idx, sequenceNumber);
                    stmt.setString(++idx, msg);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public SinkInfo<PriceDto> buildSink() {
        return new SinkInfo<>("priceSink", build());
    }
}
```
