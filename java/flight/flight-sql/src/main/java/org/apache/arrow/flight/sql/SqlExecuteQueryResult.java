package org.apache.arrow.flight.sql;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.util.List;

public class SqlExecuteQueryResult implements QueryResult {
    private final ResultManifest resultMetadata;
    private final ResultData resultData;

    public SqlExecuteQueryResult(ResultManifest resultMetadata, ResultData resultData) {
        this.resultMetadata = resultMetadata;
        this.resultData = resultData;
    }

    @Override
    public List<List<String>> getRows() {
        return this.resultData.getDataArray();
    }

    @Override
    public ResultManifest getResultMetadata() {
        return this.resultMetadata;
    }

    @Override
    public String toString() {
        return "SqlExecuteQueryResult{" +
                "resultMetadata=" + resultMetadata +
                ", resultData=" + resultData +
                '}';
    }
}
