package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.helidon.metrics.RegistryFactory;
import io.opentracing.Span;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.data.access.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Valuation;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static java.util.Optional.ofNullable;

public class GoogleDataAccessService extends AbstractDataAccessService {

    private static final Logger LOG = LoggerFactory.getLogger(GoogleDataAccessService.class);
    private static final String WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";
    private static final String READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";

    private final Counter gcsReadScopedAccessTokenCount;
    private final Counter gcsWriteScopedAccessTokenCount;
    private final int tokenLifetime;

    public GoogleDataAccessService(Config config) {
        super(config);
        RegistryFactory metricsRegistry = RegistryFactory.getInstance();
        MetricRegistry appRegistry = metricsRegistry.getRegistry(MetricRegistry.Type.APPLICATION);
        this.gcsReadScopedAccessTokenCount = appRegistry.counter("gcsReadScopedAccessTokenCount");
        this.gcsWriteScopedAccessTokenCount = appRegistry.counter("gcsWriteScopedAccessTokenCount");
        this.tokenLifetime = config.get("token.lifetime").asInt().orElse(0);
    }

    @Override
    public CompletableFuture<AccessToken> getReadAccessToken(Span span, String userId, String parentUriString) {
        CompletableFuture<AccessToken> future = new CompletableFuture<>();
        try {
            span.log(String.format("User %s is asking to read from %s", userId, parentUriString));
            LOG.info(String.format("User %s is asking to read from %s", userId, parentUriString));
            final URI parentUri = URI.create(parentUriString);
            if ("gs".equals(parentUri.getScheme())) {
                GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true,
                        getRoute(parentUri.getScheme(), ofNullable(parentUri.getAuthority()).orElse("")).getAuth().get("read"), tokenLifetime, READ_SCOPE);
                AccessToken accessToken = new AccessToken(
                        credential.getAccessToken(),
                        credential.getExpirationTime(),
                        parentUriString
                );
                future.complete(accessToken);
                gcsReadScopedAccessTokenCount.inc();
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
            LOG.info("Got route: " + route.getUri());
            if ("gs".equals(route.getUri().getScheme())) {
                GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true,
                        route.getAuth().get("write"), tokenLifetime, WRITE_SCOPE);
                AccessToken accessToken = new AccessToken(
                        credential.getAccessToken(),
                        credential.getExpirationTime(),
                        route.getUri().toString()
                );
                future.complete(accessToken);
                gcsWriteScopedAccessTokenCount.inc();
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
