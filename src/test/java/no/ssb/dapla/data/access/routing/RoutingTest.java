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
    public void testIncludeBasedValuation() {
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

    @Test
    public void testExcludeBasedValuation() {
        assertEquals("/data/datastore/not-so-sensitive-rawdata", routing.route(DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/raw/sirius/junit/test2")
                        .setVersion(System.currentTimeMillis())
                        .build())
                .setValuation(DatasetMeta.Valuation.INTERNAL)
                .setState(DatasetMeta.DatasetState.RAW)
                .build())
                .map(target -> target.get("uri"))
                .map(url -> url.get("path-prefix"))
                .map(JsonNode::textValue)
                .orElseThrow());
    }

    @Test
    public void testSpecificPathAllValuationsAndStates() {
        assertEquals("/data/datastore/tmp", routing.route(DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/tmp/junit/test3")
                        .setVersion(System.currentTimeMillis())
                        .build())
                .setValuation(DatasetMeta.Valuation.INTERNAL)
                .setState(DatasetMeta.DatasetState.RAW)
                .build())
                .map(target -> target.get("uri"))
                .map(url -> url.get("path-prefix"))
                .map(JsonNode::textValue)
                .orElseThrow());
    }

    @Test
    public void testCatchAll() {
        assertEquals("/data/datastore/catch-all", routing.route(DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/anywhere/junit/test4")
                        .setVersion(System.currentTimeMillis())
                        .build())
                .setValuation(DatasetMeta.Valuation.INTERNAL)
                .setState(DatasetMeta.DatasetState.RAW)
                .build())
                .map(target -> target.get("uri"))
                .map(url -> url.get("path-prefix"))
                .map(JsonNode::textValue)
                .orElseThrow());
    }

    @Test
    public void testExcludeBasedCatchAll() {
        assertEquals("/data/datastore/catch-all", routing.route(DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath("/raw/skatt/weird-special/junit/test5")
                        .setVersion(System.currentTimeMillis())
                        .build())
                .setValuation(DatasetMeta.Valuation.INTERNAL)
                .setState(DatasetMeta.DatasetState.RAW)
                .build())
                .map(target -> target.get("uri"))
                .map(url -> url.get("path-prefix"))
                .map(JsonNode::textValue)
                .orElseThrow());
    }
}
