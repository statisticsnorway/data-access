package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.net.URI;
import java.util.Collections;
import java.util.NoSuchElementException;
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

    /**
     * Find the first matching route based on the given source parameters
     * @param path
     * @param valuation
     * @param state
     * @return the first matching route
     */
    Route getRoute(String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        return route(path, valuation, state).orElseThrow(() ->
                new NoSuchElementException("Could not find route for path: " + path + " with valuation " + valuation +
                " and state " + state));
    }

    /**
     * Find the first matching route that has resolves to the following target scheme and host
     * @param scheme
     * @param host
     * @return the first matching route
     */
    Route getRoute(String scheme, String host) {
        return routing.asNodeList().orElseThrow(() ->
                new RuntimeException("Route configuration is missing")).stream().filter(route ->
                    route.get("target").get("uri").get("scheme").asString().get().equals(scheme) &&
                    route.get("target").get("uri").get("host").asString().get().equals(host)
        ).findFirst().map(Route::new).orElseThrow(() ->
                new NoSuchElementException("Could not find target: " + scheme + "://" + host));
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
