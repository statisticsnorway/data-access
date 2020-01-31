package no.ssb.dapla.data.access.service;

import io.helidon.common.http.Http;
import io.helidon.common.http.MediaType;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.helidon.application.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.*;

public class DataAccessHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(DataAccessHttpService.class);
    private final DataAccessService dataAccessService;

    public DataAccessHttpService(DataAccessService dataAccessService) {
        this.dataAccessService = dataAccessService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/{location}", this::httpGetAccessToken);
    }

    private void httpGetAccessToken(ServerRequest request, ServerResponse response) {
        Span span = Tracing.spanFromHttp(request, "doGet");
        try {
            String location = request.path().param("location");
            span.setTag("location", location);
            String userId = request.queryParams().first("userId").orElseThrow();
            span.setTag("userId", userId);
            AccessTokenRequest.Privilege privilege = AccessTokenRequest.Privilege.valueOf(request.queryParams()
                    .first("privilege").orElseThrow());
            span.setTag("privilege", privilege.name());

            dataAccessService.getAccessToken(span, userId, privilege, location)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(token -> {
                        if (token == null) {
                            response.status(Http.Status.NOT_FOUND_404).send();
                        } else {
                            response.headers().contentType(MediaType.APPLICATION_JSON);
                            response.send(AccessTokenResponse.newBuilder()
                                    .setAccessToken(token.getAccessToken())
                                    .setExpirationTime(token.getExpirationTime()).build());
                        }
                        span.finish();
                    }).exceptionally(t -> {
                try {
                    logError(span, t);
                    LOG.error(String.format("httpGetAccessToken() failed for user %s and location %s",
                            userId, location), t);
                    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    return null;
                } finally {
                    span.finish();
                }
            });
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e);
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }
}
