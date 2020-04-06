package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Valuation;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static java.util.Optional.*;

public class GoogleDataAccessService extends AbstractDataAccessService {

    private static final String WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";
    private static final String READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";

    public GoogleDataAccessService(Config config) {
        super(config);
    }

    @Override
    public CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUriString) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        try {
            span.log(String.format("User %s is asking to read from %s", userId, parentUriString));
            final URI parentUri = URI.create(parentUriString);
            if ("gs".equals(parentUri.getScheme())) {
                GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true,
                        getRoute(parentUri.getScheme(), ofNullable(parentUri.getAuthority()).orElse("")).getAuth().get("read"), READ_SCOPE);
                AccessToken accessToken = new AccessToken(
                        credential.getAccessToken(),
                        credential.getExpirationTime(),
                        parentUriString
                );
                future.complete(accessToken);
            } else {
                // No GCS scheme
                future.complete(null);
            }
        } catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, Valuation valuation, DatasetState state) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        try {
            span.log(String.format("User %s is asking to write path %s", userId, path));
            Route route = getRoute(path, valuation, state);
            if ("gs".equals(route.getUri().getScheme())) {
                GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true,
                        route.getAuth().get("write"), WRITE_SCOPE);
                AccessToken accessToken = new AccessToken(
                        credential.getAccessToken(),
                        credential.getExpirationTime(),
                        route.getUri().toString()
                );
                future.complete(accessToken);
            } else {
                // No GCS scheme
                future.complete(null);
            }
        } catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
        return future;
    }

}
