package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta.DatasetState;
import no.ssb.dapla.dataset.api.DatasetMeta.Valuation;

import java.util.concurrent.CompletableFuture;

public interface DataAccessService {
    CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String path, Valuation valuation, DatasetState state);

    CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, Valuation valuation, DatasetState state);
}
