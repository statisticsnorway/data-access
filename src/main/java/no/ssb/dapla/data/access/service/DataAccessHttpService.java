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
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;

public class DataAccessHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(DataAccessHttpService.class);
    private final DataAccessService dataAccessService;

    public DataAccessHttpService(DataAccessService dataAccessService) {
        this.dataAccessService = dataAccessService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/token", this::httpGetAccessToken);
        rules.get("/location", this::httpGetLocation);
    }

    private void httpGetAccessToken(ServerRequest request, ServerResponse response) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromHttp(request, "httpGetAccessToken");
        Span span = tracerAndSpan.span();
        try {
            String location = request.queryParams().first("location").orElseThrow();
            span.setTag("location", location);
            String userId = request.queryParams().first("userId").orElseThrow();
            span.setTag("userId", userId);
            AccessTokenRequest.Privilege privilege = AccessTokenRequest.Privilege.valueOf(request.queryParams()
                    .first("privilege").orElseThrow());
            span.setTag("privilege", privilege.name());

            dataAccessService.getAccessToken(span, userId, privilege, location)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(token -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
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
                    Tracing.restoreTracingContext(tracerAndSpan);
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

    private void httpGetLocation(ServerRequest request, ServerResponse response) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromHttp(request, "httpGetLocation");
        Span span = tracerAndSpan.span();
        try {
            LocationRequest.Valuation valuation = LocationRequest.Valuation.valueOf(request.queryParams()
                    .first("valuation").orElseThrow());
            span.setTag("valuation", valuation.name());
            LocationRequest.DatasetState datasetState = LocationRequest.DatasetState.valueOf(request.queryParams()
                    .first("state").orElseThrow());
            span.setTag("state", datasetState.name());

            dataAccessService.getLocation(span, valuation, datasetState)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(location -> {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        if (location == null) {
                            response.status(Http.Status.NOT_FOUND_404).send();
                        } else {
                            response.headers().contentType(MediaType.APPLICATION_JSON);
                            response.send(LocationResponse.newBuilder()
                                    .setLocation(location).build());
                        }
                        span.finish();
                    }).exceptionally(t -> {
                try {
                    Tracing.restoreTracingContext(tracerAndSpan);
                    logError(span, t);
                    LOG.error(String.format("httpGetLocation() failed for valuation %s and state %s",
                            valuation, datasetState), t);
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
