package no.ssb.dapla.data.access.service;

import io.opentracing.Span;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;

import java.util.concurrent.CompletableFuture;

public class GoogleDataAccessService implements DataAccessService {

    private static final String READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";
    private static final String WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

    @Override
    public CompletableFuture<AccessToken> getAccessToken(Span span, String userId, AccessTokenRequest.Privilege privilege,
                                                  String location) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        try {
            span.log(String.format("User %s is asking for %s privilege to location %s", userId, privilege.name(), location));
            GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true, getScope(privilege));
            AccessToken accessToken = new AccessToken(credential.getAccessToken(), credential.getExpirationTime());
            future.complete(accessToken);
        } catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private String getScope(AccessTokenRequest.Privilege privilege) {
        switch (privilege) {
            case READ:
                return WRITE_SCOPE;
                // Fix after creating new gcs connector
                //return READ_SCOPE;
            case WRITE:
                return WRITE_SCOPE;
            default:
                throw new RuntimeException("Unvalid privilege " + privilege.name());
        }
    }
}
