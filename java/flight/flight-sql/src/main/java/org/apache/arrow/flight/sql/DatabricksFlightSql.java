package org.apache.arrow.flight.sql;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.cli.*;

/**
 * start a flight sql service server using DatabricksFlighSqlProducer
 * listening 8081 port
 */
public class DatabricksFlightSql {
    public static void main(String[] args) {
        final Options options = new Options();
        options.addRequiredOption("sqlEndpointServer", "sqlEndpointServer", true, "SQL endpoint to connect to");
        options.addRequiredOption("sqlEndpointHttpPath", "sqlEndpointHttpPath", true, "SQL HTTP path to connect to");
        options.addRequiredOption("token", "token", true, "Token to use");
        CommandLine cmd = null;
        try {
            cmd = new DefaultParser().parse(options, args, /* stopAtNonOption */ true);
        } catch (final ParseException e) {
            throw new RuntimeException(e);
        }
        FlightSqlProducer producer = new DatabricksFlightSqlProducer(
                cmd.getOptionValue("sqlEndpointServer"),
                cmd.getOptionValue("sqlEndpointHttpPath"),
                cmd.getOptionValue("token")
        );

        FlightServer.Builder builder = FlightServer
                .builder()
                .allocator(new org.apache.arrow.memory.RootAllocator())
                .producer(producer)
                .location(Location.forGrpcInsecure("localhost", 8081));

        // Start the server
        FlightServer server = builder.build();

        // Adding a shutdown hook to handle CTRL-C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down the server...");
            server.shutdown();
            System.out.println("Server shut down successfully.");
        }));

        try {
            server.start();
            System.out.println("Server started. Press CTRL-C to stop.");
            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Server terminated with an exception: " + e.getMessage());
        }
    }
}
