package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.util.concurrent.CompletableFuture;

public class LocalstackDataAccessService implements DataAccessService {

    @Override
    public CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        return CompletableFuture.completedFuture(
                new AccessToken(
                        "localstack-read-token",
                        System.currentTimeMillis() + 1000 * 60 * 60,
                        System.getenv().get("DATA_ACCESS_SERVICE_DEFAULT_LOCATION") //TODO: Implement routing based on valuation and state
                )
        );
    }

    @Override
    public CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        return CompletableFuture.completedFuture(
                new AccessToken(
                        "localstack-write-token",
                        System.currentTimeMillis() + 1000 * 60 * 60,
                        System.getenv().get("DATA_ACCESS_SERVICE_DEFAULT_LOCATION") //TODO: Implement routing based on valuation and state
                )
        );
    }
}
