package org.apache.arrow.flight.sql.example;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;

public class DatabricksDemoClient {
    private static FlightSqlClient flightSqlClient;
    public static void main(final String[] args) throws Exception {
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

        final Location clientLocation = Location.forGrpcInsecure("localhost", 8081);
        flightSqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());

        flightSqlClient.executeUpdate("insert into hive_metastore.default.test_flight values ('b2', 'b3')", getCallOptions());

        FlightInfo info = flightSqlClient.execute("select c1 from hive_metastore.default.test_flight");

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
