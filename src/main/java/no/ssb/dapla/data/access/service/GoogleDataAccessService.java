package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.dataset.api.DatasetMeta;

import java.util.concurrent.CompletableFuture;

public class GoogleDataAccessService implements DataAccessService {

    private static final String READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";
    private static final String WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

    @Override
    public CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String path, DatasetMeta.Valuation valuation, DatasetMeta.DatasetState state) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        try {
            span.log(String.format("User %s is asking to read location %s", userId, path));
            GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true, READ_SCOPE);
            AccessToken accessToken = new AccessToken(
                    credential.getAccessToken(),
                    credential.getExpirationTime(),
                    System.getenv().get("DATA_ACCESS_SERVICE_DEFAULT_LOCATION") //TODO: Implement routing based on valuation and state
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
            span.log(String.format("User %s is asking to write location %s", userId, path));
            GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true, WRITE_SCOPE);
            AccessToken accessToken = new AccessToken(
                    credential.getAccessToken(),
                    credential.getExpirationTime(),
                    System.getenv().get("DATA_ACCESS_SERVICE_DEFAULT_LOCATION") //TODO: Implement routing based on valuation and state
            );
            future.complete(accessToken);
        } catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
