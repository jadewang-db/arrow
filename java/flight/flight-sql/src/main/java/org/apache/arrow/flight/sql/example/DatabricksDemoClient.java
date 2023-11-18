package org.apache.arrow.flight.sql.example;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DatabricksDemoClient {
    private static FlightSqlClient flightSqlClient;
    public static void main(final String[] args) throws Exception {
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final Location clientLocation = Location.forGrpcInsecure("localhost", 8081);
        flightSqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
        long uniqueId = Instant.now().getEpochSecond();
        String statement = String.format(
                "insert into hive_metastore.default.test_flight values ('c1_%s', 'c2_%s')",
                uniqueId, uniqueId + 1);
        long affectedRows = flightSqlClient.executeUpdate(statement, getCallOptions());
        System.out.printf("Executed %s%nAffectedRows: %s%n", statement, affectedRows);
        statement = String.format("select c1 from hive_metastore.default.test_flight where c1 like '%%%s'", uniqueId);
        FlightInfo info = flightSqlClient.execute(statement);
        System.out.printf("Executed %s%n", statement);
        printFlightInfoResults(info);
    }

    public static List<CallOption> callOptions = new ArrayList<>();
    public static CallOption[] getCallOptions() {
        return callOptions.toArray(new CallOption[0]);
    }
    private static void printFlightInfoResults(final FlightInfo flightInfo) throws Exception {
        final FlightStream stream =
                flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), getCallOptions());
        while (stream.next()) {
            try (final VectorSchemaRoot root = stream.getRoot()) {
                System.out.println(root.contentToTSVString());
            }
        }
        stream.close();
    }
}
