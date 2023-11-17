package org.apache.arrow.flight.sql;

import com.databricks.sdk.service.sql.ResultManifest;
import java.util.List;

public interface QueryResult {
    List<List<String>> getRows();
    ResultManifest getResultMetadata();
}
