package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public abstract class AbstractDataAccessService implements DataAccessService {

    private final Config routing;

    public AbstractDataAccessService(Config config) {
        routing = config.get("routing").detach();
    }

    @Override
    public abstract CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUri);

    @Override
    public abstract CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path,
                                                                       DatasetMeta.Valuation valuation,
                                                                       DatasetMeta.DatasetState state);

    @Override
    public CompletableFuture<URI> getWriteLocation(Span span, String userId, String path,
                                                      DatasetMeta.Valuation valuation,
                                                      DatasetMeta.DatasetState state) {
        return CompletableFuture.completedFuture(getRoute(path, valuation, state).getUri());
    }

    Route getRoute(String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        return route(path, valuation, state).get();
    }

    String getToken(String location) {
        //TODO: Find target with the given location and return auth
        return "";
    }

    private Optional<Route> route(String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        return routing.asNodeList().orElseThrow(() ->
                    new RuntimeException("Route configuration is missing")).stream().filter(route ->
                matchRoutingEntry(path, valuation, state, route.get("source"))).findFirst().map(Route::new);
    }

    private boolean matchRoutingEntry(String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state,
                                      Config source) {
        if (source.get("paths").exists() && !match(source.get("paths"), path::startsWith)) {
            return false;
        }
        if (source.get("valuations").exists() && !match(source.get("valuations"), valuation.name()::equalsIgnoreCase)) {
            return false;
        }
        if (source.get("states").exists() && !match(source.get("states"), state.name()::equalsIgnoreCase)) {
            return false;
        }
        return true; // all criteria matched
    }

    private boolean match(Config criterionNode, Function<String, Boolean> matcher) {
        if (criterionNode.get("excludes").asList(String.class).orElseGet(Collections::emptyList).stream().anyMatch(v -> matcher.apply(v))) {
            return false;
        }

        if (!criterionNode.get("includes").exists() || criterionNode.get("includes").asList(String.class).get().stream().anyMatch(v -> matcher.apply(v))) {
            return true;
        }

        return false; // non-empty include set, but no matches
    }

}
