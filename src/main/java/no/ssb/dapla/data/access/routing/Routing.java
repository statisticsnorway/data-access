package no.ssb.dapla.data.access.routing;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public class Routing {

    final JsonNode routingTable;

    public Routing(JsonNode routingTable) {
        this.routingTable = routingTable;
    }

    public Optional<JsonNode> route(DatasetMeta datasetMeta) {
        for (Iterator<JsonNode> it = routingTable.get("routing").elements(); it.hasNext(); ) {
            JsonNode entry = it.next();
            if (matchRoutingEntry(datasetMeta, entry)) {
                return ofNullable(entry.get("target"));
            }
        }
        return Optional.empty();
    }

    private boolean matchRoutingEntry(DatasetMeta datasetMeta, JsonNode entry) {
        String path = datasetMeta.getId().getPath();
        if (!match(ofNullable(entry.get("source")).map(n -> n.get("paths")), path::startsWith)) {
            return false;
        }
        String valuation = datasetMeta.getValuation().name();
        if (!match(ofNullable(entry.get("source")).map(n -> n.get("valuations")), valuation::equalsIgnoreCase)) {
            return false;
        }
        String state = datasetMeta.getState().name();
        if (!match(ofNullable(entry.get("source")).map(n -> n.get("states")), state::equalsIgnoreCase)) {
            return false;
        }
        return true; // all criteria matched
    }

    private boolean match(Optional<JsonNode> criterionNode, Function<String, Boolean> matcher) {
        Iterator<JsonNode> excludesNode = criterionNode.map(c -> c.get("excludes")).map(JsonNode::elements).orElseGet(Collections::emptyIterator);
        for (; excludesNode.hasNext(); ) {
            JsonNode excludePathNode = excludesNode.next();
            if (matcher.apply(excludePathNode.textValue())) {
                return false; // exclude matches
            }
        }
        Iterator<JsonNode> includesNode = criterionNode.map(c -> c.get("includes")).map(JsonNode::elements).orElseGet(Collections::emptyIterator);
        if (!includesNode.hasNext()) {
            return true; // empty include set always matches
        }
        for (; includesNode.hasNext(); ) {
            JsonNode includePathNode = includesNode.next();
            if (matcher.apply(includePathNode.textValue())) {
                return true; // include matches
            }
        }
        return false; // non-empty include set, but no matches
    }
}
