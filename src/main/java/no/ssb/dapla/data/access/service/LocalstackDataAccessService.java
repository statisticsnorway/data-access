package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.data.access.protobuf.DatasetState;
import no.ssb.dapla.data.access.protobuf.Privilege;
import no.ssb.dapla.data.access.protobuf.Valuation;

import java.util.concurrent.CompletableFuture;

public class LocalstackDataAccessService implements DataAccessService {

    @Override
    public CompletableFuture<AccessToken> getAccessToken(Span span, String userId, Privilege privilege, String path, Valuation valuation, DatasetState state) {
        return CompletableFuture.completedFuture(
                new AccessToken(
                        "localstack-token",
                        System.currentTimeMillis() + 1000 * 60 * 60,
                        System.getenv().get("DATA_ACCESS_SERVICE_DEFAULT_LOCATION") //TODO: Implement routing based on valuation and state
                )
        );
    }

}
