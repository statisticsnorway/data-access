package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.opentracing.Span;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

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
            GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true,
                    getRoute(parentUri.getScheme(), parentUri.getAuthority()).getAuth().get("read"), READ_SCOPE);
            AccessToken accessToken = new AccessToken(
                    credential.getAccessToken(),
                    credential.getExpirationTime(),
                    parentUriString
            );
            future.complete(accessToken);
        } catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<AccessToken> getWriteAccessToken(Span span, String userId, String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        try {
            span.log(String.format("User %s is asking to write path %s", userId, path));
            Route route = getRoute(path, valuation, state);
            GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true,
                    route.getAuth().get("write"), WRITE_SCOPE);
            AccessToken accessToken = new AccessToken(
                    credential.getAccessToken(),
                    credential.getExpirationTime(),
                    route.toString()
            );
            future.complete(accessToken);
        } catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
        return future;
    }

}
