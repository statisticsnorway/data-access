package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.dataset.api.DatasetMeta.DatasetState;
import no.ssb.dapla.dataset.api.DatasetMeta.Valuation;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public interface DataAccessService {
    CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUri);

    CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, Valuation valuation, DatasetState state);

    CompletableFuture<URI> getWriteLocation(Span span, String userId, String path, Valuation valuation, DatasetState state);
}
