package org.apache.arrow.flight.sql;

import com.google.protobuf.Message;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import com.databricks.sdk.client.DatabricksConfig;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Any.pack;
import static java.util.Collections.singletonList;

public class DatabricksFlightSqlProducer extends BasicFlightSqlProducer {
    private final RestDatabricksClient restDatabricksClient;

    private DatabricksResultSet resultSet = null;
    private Schema schema = null;

    public DatabricksFlightSqlProducer(String sqlEndpointServer, String sqlEndpointHttpPath, String token) {
        DatabricksConfig config = new DatabricksConfig()
                .setToken(token)
                .setHost("https://" + sqlEndpointServer);
        this.restDatabricksClient = new RestDatabricksClient(config, sqlEndpointHttpPath);
    }

    private final BufferAllocator rootAllocator = new RootAllocator();
    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor flightDescriptor, Schema schema) {
        final Ticket ticket = new Ticket(pack(request).toByteArray());
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, Location.forGrpcInsecure("localhost", 8081)));

        return endpoints;
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        super.getStreamCatalogs(context, listener);
    }

    @Override
    public FlightInfo getFlightInfoStatement(
            final FlightSql.CommandStatementQuery request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(request.getQueryBytes())
                .build();
        this.resultSet = DatabricksResultSet.of(restDatabricksClient.executeStatement(request.getQuery()));
        final Ticket t = new Ticket(pack(ticket).toByteArray());
        final List<FlightEndpoint> endpoints = singletonList(
                new FlightEndpoint(t, Location.forGrpcInsecure("localhost", 8081)));
        this.schema = new Schema(extractColumns(resultSet.getMetaData()));
        FlightInfo info = new FlightInfo(schema, descriptor, endpoints, -1, -1);
        return info;
    }

    private static ArrowType.PrimitiveType toArrowType(int columnType) {
        if (columnType == Types.INTEGER) return new ArrowType.Int(32, true);
        if (columnType == Types.BOOLEAN) return new ArrowType.Bool();
        if (columnType == Types.VARCHAR) return new ArrowType.Utf8();
        if (columnType == Types.DECIMAL) return new ArrowType.Int(32, true);
        if (columnType == Types.DATE) return new ArrowType.Date(
                org.apache.arrow.vector.types.DateUnit.DAY);
        if (columnType == Types.DOUBLE) return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (columnType == Types.FLOAT) return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (columnType == Types.BIGINT) return new ArrowType.Int(64, true);
        if (columnType == Types.TIMESTAMP) return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        return new ArrowType.Int(32, true);
    }

    private static List<Field> extractColumns(ResultSetMetaData metadata) {
        List<Field> fields = new ArrayList<>();
        try {
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                fields.add(Field.nullablePrimitive(
                        metadata.getColumnName(i + 1),
                        toArrowType(metadata.getColumnType(i + 1))
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return fields;
    }

    @Override
    public void getStreamStatement(final FlightSql.TicketStatementQuery ticketStatementQuery, final CallContext context,
                                   final ServerStreamListener listener) {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
            final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
            listener.start(vectorSchemaRoot);
            loader.load(createRecordBatch(schema, resultSet, rootAllocator));
            listener.putNext();
            vectorSchemaRoot.clear();
            listener.putNext();
            listener.completed();
        }
    }

    private static ArrowRecordBatch createRecordBatch(
            Schema schema, DatabricksResultSet resultSet, BufferAllocator allocator) {
        int rowCount = resultSet.getRows().size();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector vector = null;
            int columnCount = schema.getFields().size();
            for (int column = 0; column < columnCount; column++) {
                Field field = schema.getFields().get(column);
                vector = (VarCharVector) root.getVector(field.getName());
                vector.allocateNew(rowCount);
                for (int row = 0; row < rowCount; row++) {
                    vector.setSafe(row, resultSet.getRows().get(row).get(column).getBytes());
                }
                vector.setValueCount(rowCount);
            }
            // Set the row count for the root
            root.setRowCount(rowCount);

            // Now, create the ArrowRecordBatch
            ArrowRecordBatch recordBatch = new ArrowRecordBatch(
                    rowCount,
                    Collections.singletonList(new ArrowFieldNode(rowCount, 0)),
                    Arrays.stream(vector.getBuffers(false)).collect(Collectors.toList()));
            return recordBatch;
        }
    }
}
