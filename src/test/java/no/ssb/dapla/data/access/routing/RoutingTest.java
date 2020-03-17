package no.ssb.dapla.data.access.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoutingTest {

    static Routing routing;

    @BeforeAll
    public static void beforeAll() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode routingTable = mapper.readValue(ClassLoader.getSystemResource("routing-table.json"), JsonNode.class);
        routing = new Routing(routingTable);
    }

    @Test
    public void test1() {
        assertEquals("/data/datastore/sensitive-rawdata", routing.route(DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/raw/sirius/junit/test1")
                        .setVersion(System.currentTimeMillis())
                        .build())
                .setValuation(DatasetMeta.Valuation.SENSITIVE)
                .setState(DatasetMeta.DatasetState.RAW)
                .build())
                .map(target -> target.get("uri"))
                .map(url -> url.get("path-prefix"))
                .map(JsonNode::textValue)
                .orElseThrow());
    }
}
