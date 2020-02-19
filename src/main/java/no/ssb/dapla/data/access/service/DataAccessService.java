package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import java.util.concurrent.CompletableFuture;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;

public interface DataAccessService {
    CompletableFuture<AccessToken> getAccessToken(Span span, String userId, AccessTokenRequest.Privilege privilege, String location);
}
