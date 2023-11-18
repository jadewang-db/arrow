package org.apache.arrow.flight.sql;

import com.databricks.sdk.DatabricksWorkspace;
import com.databricks.sdk.client.DatabricksConfig;
import com.databricks.sdk.service.sql.ExecuteStatementResponse;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementState;

public class RestDatabricksClient implements DatabricksClient {
    private final DatabricksWorkspace ws;
    private final String httpPath;
    private final String warehouseId;

    public RestDatabricksClient(DatabricksConfig config, String httpPath) {
        ws = new DatabricksWorkspace(config);
        this.httpPath = httpPath;
        this.warehouseId = getWarehouse(httpPath);
    }

    static private String getWarehouse(String httpPath) {
        String[] parts = httpPath.split("/");
        return parts[parts.length - 1];
    }

    @Override
    public QueryResult executeStatement(String sql) {
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setStatement(sql)
                .setWarehouseId(this.warehouseId);

        try {
            ExecuteStatementResponse response = ws.statementExecution().executeStatement(request);
            if (response.getStatus().getState() != StatementState.SUCCEEDED) {
                System.out.printf("Failed to execute request. Resp: %s, %s", response.getStatementId(), response.getStatus());
            }
            return new SqlExecuteQueryResult(
                    response.getManifest(), response.getResult());
        } catch (Exception e) {
            System.out.printf("Failed to execute request %s", e.getMessage());
            throw e;
        }
    }

    @Override
    public String toString() {
        return "RestDatabricksClient{" +
                "ws=" + ws +
                ", httpPath='" + httpPath + '\'' +
                ", warehouseId='" + warehouseId + '\'' +
                '}';
    }
}
