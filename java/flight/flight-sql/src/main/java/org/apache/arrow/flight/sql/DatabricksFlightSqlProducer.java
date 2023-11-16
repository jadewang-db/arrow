package org.apache.arrow.flight.sql;

import com.google.protobuf.Message;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class DatabricksFlightSqlProducer extends BasicFlightSqlProducer {
    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor flightDescriptor, Schema schema) {
        return null;
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        super.getStreamCatalogs(context, listener);
    }
}
