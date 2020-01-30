package no.ssb.dapla.data.access.service;

import io.opentracing.Span;

import java.util.concurrent.CompletableFuture;

import no.ssb.dapla.data.access.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.LocationRequest;

public class DataAccessService {

    private static final String READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";
    private static final String WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";
    private static final String DEFAULT_LOCATION = "DATA_ACCESS_SERVICE_DEFAULT_LOCATION";

    public DataAccessService() {
    }

    CompletableFuture<AccessToken> getAccessToken(Span span, String userId, AccessTokenRequest.Privilege privilege,
                                             String location) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        span.log(String.format("User %s is asking for % privilege to location %s", userId, privilege.name(), location));
        GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true, getScope(privilege));
        AccessToken accessToken =  new AccessToken(credential.getAccessToken(), credential.getExpirationTime());
        future.complete(accessToken);
        return future;
    }

    private String getScope(AccessTokenRequest.Privilege privilege) {
        switch (privilege) {
            case READ:
                return READ_SCOPE;
            case WRITE:
                return WRITE_SCOPE;
            default:
                throw new RuntimeException("Unvalid privilege " + privilege.name());
        }
    }

    CompletableFuture<String> getLocation(Span span, LocationRequest.Valuation valuation,
                                          LocationRequest.DatasetState state) {
        CompletableFuture<String> future = new CompletableFuture<>();
        span.log(String.format("Calling getLocation with valuation %s and state %s", valuation.name(), state.name()));
        future.complete(System.getenv().get(DEFAULT_LOCATION));
        return future;
    }
}
