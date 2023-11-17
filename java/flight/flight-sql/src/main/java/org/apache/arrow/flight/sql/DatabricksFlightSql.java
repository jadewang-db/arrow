package org.apache.arrow.flight.sql;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;

/**
 * start a flight sql service server using DatabricksFlighSqlProducer
 * listening 8081 port
 */
public class DatabricksFlightSql {
    private final static String sqlEndpointServer = "e2-dogfood.staging.cloud.databricks.com";
    private final static String sqlEndpointHttpPath = "/sql/1.0/endpoints/5c89f447c476a5a8";
    private final static String PAT = "dapi" + "fcbeefdf48a49dfa50c648c44bfe279c";

    public static void main(String[] args) {
        FlightSqlProducer producer = new DatabricksFlightSqlProducer(
                sqlEndpointServer, sqlEndpointHttpPath, PAT
        );

        FlightServer.Builder builder = FlightServer
                .builder()
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
