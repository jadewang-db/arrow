package org.apache.arrow.flight.sql;

import com.google.protobuf.Message;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.lang.model.type.PrimitiveType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Any.pack;
import static java.util.Collections.list;
import static java.util.Collections.singletonList;

public class DatabricksFlightSqlProducer extends BasicFlightSqlProducer {

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

        final Ticket t = new Ticket(pack(ticket).toByteArray());
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(t, Location.forGrpcInsecure("localhost", 8081)));

        List<Field> fields = List.of(Field.nullablePrimitive("id", new ArrowType.Int(32, true)));
        Schema schema = new Schema(fields);

        FlightInfo info = new FlightInfo(schema, descriptor, endpoints, -1, -1);

        return info;
    }

    @Override
    public void getStreamStatement(final FlightSql.TicketStatementQuery ticketStatementQuery, final CallContext context,
                                   final ServerStreamListener listener) {

        List<Field> fields = List.of(Field.nullablePrimitive("id", new ArrowType.Int(32, true)));
        Schema schema = new Schema(fields);
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
            final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
            listener.start(vectorSchemaRoot);
            loader.load(createRecordBatch(rootAllocator, 10));
            listener.putNext();
            vectorSchemaRoot.clear();
            listener.putNext();
            listener.completed();
        }
    }

    private static ArrowRecordBatch createRecordBatch(BufferAllocator allocator, int numElements) {
        // Define a schema for the batch (one int column)
        Field intField = new Field("intColumn", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Collections.singletonList(intField));

        // Create a VectorSchemaRoot based on the schema
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            // Get the IntVector and allocate memory for numElements integers
            IntVector intVector = (IntVector) root.getVector("intColumn");
            intVector.allocateNew(numElements);

            // Populate the IntVector with sample data
            for (int i = 0; i < numElements; i++) {
                intVector.setSafe(i, i);
            }
            intVector.setValueCount(numElements);

            // Set the row count for the root
            root.setRowCount(numElements);

            // Now, create the ArrowRecordBatch
            ArrowRecordBatch recordBatch = new ArrowRecordBatch(
                    numElements,
                    Collections.singletonList(new ArrowFieldNode(numElements, 0)),
                    Arrays.stream(intVector.getBuffers(false)).collect(Collectors.toList()));

            return recordBatch;
        }
    }
}
