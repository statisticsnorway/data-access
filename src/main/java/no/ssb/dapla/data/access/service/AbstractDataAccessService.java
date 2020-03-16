package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractDataAccessService implements DataAccessService {

    private final Map<String, String> buckets;
    private final Config mapping;

    public AbstractDataAccessService(Config config) {
        buckets = config.get("bucket").detach().asMap().get();
        mapping = config.get("mapping").detach();
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
        return CompletableFuture.completedFuture(getLocation(valuation, state));
    }

    URI getLocation(DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        return mapping.get(valuation.name()).get(state.name()).as(name ->
                URI.create("gs://" + name.asNode().get().asString().get())).orElseThrow(() ->
                new RuntimeException(String.format("Mapping configuration for valuation %s and state %s is missing",
                        valuation.name(), state.name())));
    }

    String getToken(String location) {
        return buckets.get(location);
    }


}
