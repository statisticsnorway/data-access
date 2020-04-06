package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Valuation;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static java.util.Optional.ofNullable;

public class MockDataAccessService extends AbstractDataAccessService {

    public MockDataAccessService(Config config) {
        super(config);
    }

    @Override
    public CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUriString) {
        final URI parentUri = URI.create(parentUriString);
        String route = getRoute(parentUri.getScheme(), ofNullable(parentUri.getAuthority()).orElse("")).getAuth().get("read");
        return CompletableFuture.completedFuture(
                new AccessToken(
                        route + "-read-token",
                        System.currentTimeMillis() + 1000 * 60 * 60,
                        parentUriString
                )
        );
    }

    @Override
    public CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, Valuation valuation, DatasetState state) {
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
