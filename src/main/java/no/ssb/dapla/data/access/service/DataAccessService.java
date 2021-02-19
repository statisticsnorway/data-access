package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Valuation;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public interface DataAccessService {
    CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUri);

    CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, Valuation valuation, DatasetState state);

    default Single<AccessToken> getWriteAccessToken(Span span, String userId, String path, String valuation, String state) {
        return Single.defer(() ->
            Single.create(getWriteAccessToken(
                    span,
                    userId,
                    path,
                    Valuation.valueOf(valuation),
                    DatasetState.valueOf(state)
            ))
        );
    }

    CompletableFuture<URI> getWriteLocation(Span span, String userId, String path, Valuation valuation, DatasetState state);
}
