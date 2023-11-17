package org.apache.arrow.flight.sql;

public interface DatabricksClient {
    QueryResult executeStatement(String sql);
}
