package org.apache.arrow.flight.sql;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
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
        System.out.printf("Executing query: %s%n", request.getQuery());
        this.resultSet = DatabricksResultSet.of(restDatabricksClient.executeStatement(request.getQuery()));
        System.out.printf("Executed query with result: %s%n", resultSet.getRows());
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

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                       FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> {
            System.out.printf("Executing query: %s%n", command.getQuery());
            DatabricksResultSet resultSet = DatabricksResultSet.of(restDatabricksClient.executeStatement(command.getQuery()));
            System.out.printf("Executed query with result: %s%n", resultSet.getRows());
            final FlightSql.DoPutUpdateResult build =
                    FlightSql.DoPutUpdateResult.newBuilder().setRecordCount(1).build();
            try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
                buffer.writeBytes(build.toByteArray());
                ackStream.onNext(PutResult.metadata(buffer));
                ackStream.onCompleted();
            }
        };
    }

    private static void writeColumn(VectorSchemaRoot root, Schema schema, int column, DatabricksResultSet resultSet) {
        int rowCount = resultSet.getRows() == null ? 0 : resultSet.getRows().size();
        Field field = schema.getFields().get(column);
        FieldVector vector = root.getVector(field.getName());
        if (vector instanceof VarCharVector) {
            VarCharVector varChar = (VarCharVector) vector;
            varChar.allocateNew();
            for (int row = 0; row < rowCount; row++) {
                varChar.setSafe(row, resultSet.getRows().get(row).get(column).getBytes());
            }
        } else if (vector instanceof IntVector) {
            IntVector intVector = (IntVector) vector;
            intVector.allocateNew(rowCount);
            for (int row = 0; row < rowCount; row++) {
                intVector.setSafe(row, Integer.parseInt(resultSet.getRows().get(row).get(column)));
            }
        } else if (vector instanceof BigIntVector) {
            BigIntVector bigIntVector = (BigIntVector) vector;
            bigIntVector.allocateNew(rowCount);
            for (int row = 0; row < rowCount; row++) {
                bigIntVector.setSafe(row, Integer.parseInt(resultSet.getRows().get(row).get(column)));
            }
        } else {
            System.out.println("Unsupported vector type: " + vector.getClass().getName());
        }
        vector.setValueCount(rowCount);
    }

    private static ArrowRecordBatch createRecordBatch(
            Schema schema, DatabricksResultSet resultSet, BufferAllocator allocator) {
        int rowCount = resultSet.getRows() == null ? 0 : resultSet.getRows().size();
        if (rowCount == 0) {
            // This could be an update command without return values.
            return new ArrowRecordBatch(
                    0,
                    Collections.emptyList(),
                    Collections.emptyList());
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            int columnCount = schema.getFields().size();
            for (int column = 0; column < columnCount; column++) {
                writeColumn(root, schema, column, resultSet);
            }
            root.setRowCount(rowCount);

            // Create an ArrowRecordBatch
            List<org.apache.arrow.memory.ArrowBuf> buffers = new ArrayList<>();
            for (FieldVector v : root.getFieldVectors()) {
                v.getFieldBuffers().forEach(buffers::add);
            }
            return new ArrowRecordBatch(
                    root.getRowCount(),
                    root.getFieldVectors().stream().map(v -> new ArrowFieldNode(rowCount, 0))
                            .collect(Collectors.toList()),
                    buffers);
        }
    }
}
