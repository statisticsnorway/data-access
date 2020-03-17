package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class LocalstackDataAccessService extends AbstractDataAccessService {

    public LocalstackDataAccessService(Config config) {
        super(config);
    }

    @Override
    public CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUri) {
        return CompletableFuture.completedFuture(
                new AccessToken(
                        URI.create(parentUri).getAuthority() + "-read-token",
                        System.currentTimeMillis() + 1000 * 60 * 60,
                        parentUri
                )
        );
    }

    @Override
    public CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        Route route = getRoute(path, valuation, state);
        return CompletableFuture.completedFuture(
                new AccessToken(
                        route.getUri().getAuthority() + "-write-token",
                        System.currentTimeMillis() + 1000 * 60 * 60,
                        route.getUri().toString()
                )
        );
    }

}
