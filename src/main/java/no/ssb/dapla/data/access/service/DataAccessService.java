package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.data.access.protobuf.DatasetState;
import no.ssb.dapla.data.access.protobuf.Privilege;
import no.ssb.dapla.data.access.protobuf.Valuation;

import java.util.concurrent.CompletableFuture;

public interface DataAccessService {
    CompletableFuture<AccessToken> getAccessToken(Span span, String userId, Privilege privilege, String path, Valuation valuation, DatasetState state);
}
