package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;

import java.util.concurrent.CompletableFuture;

public class LocalstackDataAccessService implements DataAccessService {

    @Override
    public CompletableFuture<AccessToken> getAccessToken(Span span, String userId, AccessTokenRequest.Privilege privilege,
                                                         String location) {
        return CompletableFuture.completedFuture(new AccessToken("localstack-token", System.currentTimeMillis() + 1000 * 60 * 60));
    }

}
