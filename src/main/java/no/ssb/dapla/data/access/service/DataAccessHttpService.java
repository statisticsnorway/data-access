package no.ssb.dapla.data.access.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.protobuf.ByteString;
import io.helidon.common.reactive.Single;
import io.helidon.metrics.RegistryFactory;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.Privilege;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.data.access.metadata.MetadataSigner;
import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetMetaAll;
import no.ssb.helidon.application.Tracing;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static no.ssb.helidon.application.Tracing.logError;

public class DataAccessHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(DataAccessHttpService.class);
    private static final String FALLBACK_TOKEN = JWT.create()
            .withClaim("preferred_username", "unknown")
            .sign(Algorithm.HMAC256("s3cr3t"));

    private final DataAccessService dataAccessService;
    private final UserAccessClient userAccessClient;
    private final CatalogClient catalogClient;

    private final MetadataSigner metadataSigner;
    private final Counter readRequestRequestCount;
    private final Counter readRequestAllowedCount;
    private final Counter readRequestDeniedCount;
    private final Counter readRequestNotFoundCount;
    private final Counter readRequestFailedCount;
    private final Counter writeRequestRequestCount;
    private final Counter writeRequestAllowedCount;
    private final Counter writeRequestDeniedCount;
    private final Counter writeRequestFailedCount;

    public DataAccessHttpService(DataAccessService dataAccessService, UserAccessClient userAccessClient,
                                 CatalogClient catalogClient, MetadataSigner metadataSigner) {
        this.dataAccessService = dataAccessService;
        this.userAccessClient = userAccessClient;
        this.catalogClient = catalogClient;
        this.metadataSigner = metadataSigner;
        RegistryFactory metricsRegistry = RegistryFactory.getInstance();
        MetricRegistry appRegistry = metricsRegistry.getRegistry(MetricRegistry.Type.APPLICATION);
        this.readRequestRequestCount = appRegistry.counter("readRequestRequestCount");
        this.readRequestAllowedCount = appRegistry.counter("readRequestAllowedCount");
        this.readRequestDeniedCount = appRegistry.counter("readRequestDeniedCount");
        this.readRequestNotFoundCount = appRegistry.counter("readRequestNotFoundCount");
        this.readRequestFailedCount = appRegistry.counter("readRequestFailedCount");
        this.writeRequestRequestCount = appRegistry.counter("writeRequestRequestCount");
        this.writeRequestAllowedCount = appRegistry.counter("writeRequestAllowedCount");
        this.writeRequestDeniedCount = appRegistry.counter("writeRequestDeniedCount");
        this.writeRequestFailedCount = appRegistry.counter("writeRequestFailedCount");
    }

    static DecodedJWT extractJWT(ServerRequest req) {
        return req.headers().value("Authorization")
                .filter(h -> h.contains("Bearer "))
                .map(h -> h.substring("Bearer ".length()))
                .map(JWT::decode)
                .orElseGet(() -> JWT.decode(FALLBACK_TOKEN));

    }

    static String extractUserId(DecodedJWT token) {
        // TODO use subject instead of preferred_username
        return token.getClaim("preferred_username").asString();
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.post("/rpc/DataAccessService/readLocation", Handler.create(ReadLocationRequest.class, this::readLocation));
        rules.post("/rpc/DataAccessService/writeLocation", Handler.create(WriteLocationRequest.class, this::writeLocation));
        rules.post("/rpc/DataAccessService/deleteLocation", Handler.create(DeleteLocationRequest.class, this::deleteLocation));
    }

    public void readLocation(ServerRequest req, ServerResponse res, ReadLocationRequest request) {
        readRequestRequestCount.inc();
        Span span = Tracing.spanFromHttp(req, "readLocation");
        try {

            DecodedJWT JWT = extractJWT(req);
            String userId = extractUserId(JWT);

            readRequest(req, res, request.getPath(), String.valueOf(request.getSnapshot()), span, JWT.getToken(), userId,
                    (getDatasetResponse, accessCheckResponse) -> {
                        try {
                            Tracing.restoreTracingContext(req.tracer(), span);

                            ReadLocationResponse.Builder responseBuilder = ReadLocationResponse.newBuilder()
                                    .setAccessAllowed(accessCheckResponse.getAllowed())
                                    .setParentUri(ofNullable(getDatasetResponse)
                                            .filter(GetDatasetResponse::hasDataset)
                                            .map(GetDatasetResponse::getDataset)
                                            .map(Dataset::getParentUri)
                                            .orElse("")
                                    )
                                    .setVersion(ofNullable(getDatasetResponse)
                                            .filter(GetDatasetResponse::hasDataset)
                                            .map(GetDatasetResponse::getDataset)
                                            .map(Dataset::getId)
                                            .map(DatasetId::getTimestamp)
                                            .map(String::valueOf)
                                            .orElse(""));

                            if (!accessCheckResponse.getAllowed()) {
                                try {
                                    res.status(200).send(responseBuilder.build()); // TODO 403
                                    readRequestDeniedCount.inc();
                                } finally {
                                    span.finish();
                                }
                                return;
                            }

                            CompletableFuture<AccessToken> accessTokenFuture = dataAccessService.getReadAccessToken(
                                    span, userId, getDatasetResponse.getDataset().getParentUri());

                            accessTokenFuture
                                    .orTimeout(10, TimeUnit.SECONDS)
                                    .thenAccept(token -> {
                                        Tracing.restoreTracingContext(req.tracer(), span);
                                        responseBuilder
                                                .setParentUri(getDatasetResponse.getDataset().getParentUri())
                                                .setVersion(String.valueOf(getDatasetResponse.getDataset().getId().getTimestamp()));
                                        if (token != null) {
                                            responseBuilder
                                                    .setAccessToken(token.getAccessToken())
                                                    .setExpirationTime(token.getExpirationTime());
                                        }
                                        res.status(200).send(responseBuilder.build());
                                        readRequestAllowedCount.inc();
                                        span.finish();
                                    })
                                    .exceptionally(throwable -> {
                                        try {
                                            Tracing.restoreTracingContext(req.tracer(), span);
                                            logError(span, throwable, "error in getReadAccessToken()");
                                            LOG.error(String.format("getReadAccessToken()"), throwable);
                                            res.status(500).send(throwable);
                                            readRequestFailedCount.inc();
                                            return null;
                                        } finally {
                                            span.finish();
                                        }
                                    });

                            span.finish();
                        } catch (RuntimeException | Error e) {
                            logError(span, e, "unexpected error");
                            LOG.error("unexpected error", e);
                            res.status(500).send(e);
                            readRequestFailedCount.inc();
                        }
                    }
            );

        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                readRequestFailedCount.inc();
                throw e;
            } finally {
                span.finish();
            }
        }
    }


    <R> void readRequest(ServerRequest req, ServerResponse res, final String path, String version, Span span, String bearerToken, String userId, BiConsumer<GetDatasetResponse, AccessCheckResponse> onUserAccessResponseConsumer) {

        // TODO Add fallback that reads metadata directly from bucket instead of catalog. That will allow this service
        //  to continue functioning even when catalog is down. Will require guessing bucket based on routing-table.

        GetDatasetRequest getDatasetRequest = GetDatasetRequest.newBuilder()
                .setPath(path)
                .setTimestamp((version == null || version.length() == 0) ? 0 : Long.parseLong(version))
                .build();

        catalogClient.get(getDatasetRequest, bearerToken).subscribe(getDatasetResponse -> {
            if (ofNullable(getDatasetResponse)
                    .filter(GetDatasetResponse::hasDataset)
                    .map(GetDatasetResponse::getDataset)
                    .map(Dataset::getId)
                    .map(DatasetId::getPath)
                    .orElse("")
                    .isBlank()) {
                // no record of dataset in catalog
                res.status(404).send();
                readRequestNotFoundCount.inc();
                return;
            }

            Dataset dataset = getDatasetResponse.getDataset();
            AccessCheckRequest accessCheckRequest = AccessCheckRequest.newBuilder()
                    .setUserId(userId)
                    .setValuation(dataset.getValuation().name())
                    .setState(dataset.getState().name())
                    .setPath(path)
                    .setPrivilege(Privilege.READ.name())
                    .build();

            userAccessClient.hasAccess(accessCheckRequest, bearerToken).subscribe(
                    accessCheckResponse -> {
                        onUserAccessResponseConsumer.accept(getDatasetResponse, accessCheckResponse);
                    },
                    throwable -> {
                        try {
                            Tracing.restoreTracingContext(req.tracer(), span);
                            logError(span, throwable, "error while performing authServiceFutureStub.hasAccess");
                            LOG.error("getAccessToken: error while performing authServiceFutureStub.hasAccess", throwable);
                            LOG.info("Access check request: " + ProtobufJsonUtils.toString(accessCheckRequest));
                            res.status(500).send(throwable);
                            readRequestFailedCount.inc();
                        } finally {
                            span.finish();
                        }
                    }
            );
        }, throwable -> {
            try {
                Tracing.restoreTracingContext(req.tracer(), span);
                logError(span, throwable, "error while performing catalog get");
                LOG.error("readRequest: error while performing catalog get", throwable);
                res.status(500).send(throwable);
                readRequestFailedCount.inc();
            } finally {
                span.finish();
            }
        });
    }

    /**
     * Handler for the deleteLocation http endpoint.
     * <p>
     * Calling this endpoint <strong>does not</strong> delete anything. It simply returns
     * the token, the routed URI (parentUri) and whether the user has write (thus delete)
     * access to a particular dataset version.
     */
    public void deleteLocation(ServerRequest req, ServerResponse res, DeleteLocationRequest deleteLocationRequest) {
        // TODO: Not really sure how to cleanly handle spans with reactive APIs..
        Span span = Tracing.spanFromHttp(req, "deleteLocation");
        try {
            DecodedJWT JWT = extractJWT(req);
            String userId = extractUserId(JWT);
            String path = deleteLocationRequest.getPath();
            Long version = deleteLocationRequest.getSnapshot();

            catalogClient.get(path, version, JWT.getToken())
                    .map(GetDatasetResponse::getDataset)
                    .flatMapSingle(dataset ->
                        userAccessClient.hasAccess(
                                userId,
                                Privilege.DELETE.name(),
                                path,
                                dataset.getValuation().name(),
                                dataset.getState().name(),
                                JWT.getToken()
                        ).flatMapSingle(accessGranted -> {
                            if (accessGranted) {
                                return dataAccessService.getWriteAccessToken(span, extractUserId(JWT),
                                        dataset.getId().getPath(),
                                        dataset.getValuation().name(),
                                        dataset.getState().name())
                                        .map(accessToken -> DeleteLocationResponse.newBuilder()
                                                .setAccessAllowed(true)
                                                .setAccessToken(accessToken.getAccessToken())
                                                .setExpirationTime(accessToken.getExpirationTime())
                                                .setParentUri(accessToken.getParentUri())
                                                .build());
                            } else {
                                return Single.just(DeleteLocationResponse.newBuilder()
                                        .setAccessAllowed(false)
                                );
                            }
                        })
                    ).thenAccept(deleteLocationResponse ->
                        res.status(200).send(deleteLocationResponse)
                    ).exceptionallyAccept(throwable ->
                        res.status(500).send(throwable)
                    ).thenRunAsync(() -> span.finish());
        } finally {
            span.finish();
        }
    }

    public void writeLocation(ServerRequest req, ServerResponse res, WriteLocationRequest request) {
        writeRequestRequestCount.inc();
        Span span = Tracing.spanFromHttp(req, "writeLocation");
        try {

            DecodedJWT JWT = extractJWT(req);
            String userId = extractUserId(JWT);

            DatasetMeta untrustedMetadata = ProtobufJsonUtils.toPojo(request.getMetadataJson(), DatasetMeta.class);

            writeRequest(req, res, untrustedMetadata, span, JWT.getToken(), userId, accessCheckResponse -> {
                try {
                    Tracing.restoreTracingContext(req.tracer(), span);
                    if (accessCheckResponse.getAllowed()) {

                        CompletableFuture<URI> writeLocationFuture = dataAccessService.getWriteLocation(
                                span, userId, untrustedMetadata.getId().getPath(), untrustedMetadata.getValuation(), untrustedMetadata.getState()
                        );

                        writeLocationFuture
                                .orTimeout(10, TimeUnit.SECONDS)
                                .thenAccept(location -> {
                                    Tracing.restoreTracingContext(req.tracer(), span);
                                    DatasetMeta allowedMetadata = DatasetMeta.newBuilder()
                                            .mergeFrom(untrustedMetadata)
                                            .setCreatedBy(userId)
                                            .build();

                                    ByteString validMetadataJson = ByteString.copyFromUtf8(ProtobufJsonUtils.toString(allowedMetadata));
                                    ByteString signature = ByteString.copyFrom(metadataSigner.sign(validMetadataJson.toByteArray()));

                                    DatasetMetaAll allowedMetadataAll = DatasetMetaAll.newBuilder()
                                            .setId(allowedMetadata.getId())
                                            .setType(allowedMetadata.getType())
                                            .setValuation(allowedMetadata.getValuation())
                                            .setState(allowedMetadata.getState())
                                            .setPseudoConfig(allowedMetadata.getPseudoConfig())
                                            .setCreatedBy(allowedMetadata.getCreatedBy())
                                            .setRandom(UUID.randomUUID().toString()) // strengthen cryptographic signature
                                            .setParentUri(location.toString())
                                            .build();

                                    ByteString allValidMetadataJson = ByteString.copyFromUtf8(ProtobufJsonUtils.toString(allowedMetadataAll));
                                    ByteString allSignature = ByteString.copyFrom(metadataSigner.sign(allValidMetadataJson.toByteArray()));

                                    CompletableFuture<AccessToken> accessTokenFuture = dataAccessService.getWriteAccessToken(
                                            span, userId, allowedMetadata.getId().getPath(), allowedMetadata.getValuation(), allowedMetadata.getState()
                                    );

                                    accessTokenFuture
                                            .orTimeout(10, TimeUnit.SECONDS)
                                            .thenAccept(token -> {
                                                Tracing.restoreTracingContext(req.tracer(), span);
                                                WriteLocationResponse.Builder responseBuilder = WriteLocationResponse.newBuilder()
                                                        .setAccessAllowed(true)
                                                        .setValidMetadataJson(validMetadataJson)
                                                        .setMetadataSignature(signature)
                                                        // parentUri contains the bucket from the routing table.
                                                        .setParentUri(allowedMetadataAll.getParentUri())
                                                        .setAllValidMetadataJson(allValidMetadataJson)
                                                        .setAllMetadataSignature(allSignature);
                                                if (token != null) {
                                                    responseBuilder
                                                            .setAccessToken(token.getAccessToken())
                                                            .setExpirationTime(token.getExpirationTime());
                                                }
                                                WriteLocationResponse responsePojo = responseBuilder.build();
                                                res.status(200).send(responsePojo);
                                                writeRequestAllowedCount.inc();
                                                span.finish();
                                            })
                                            .exceptionally(throwable -> {
                                                try {
                                                    Tracing.restoreTracingContext(req.tracer(), span);
                                                    logError(span, throwable, "error in getWriteAccessToken()");
                                                    LOG.error(String.format("getWriteAccessToken()"), throwable);
                                                    res.status(500).send(throwable);
                                                    writeRequestFailedCount.inc();
                                                    return null;
                                                } finally {
                                                    span.finish();
                                                }
                                            });

                                })
                                .exceptionally(throwable -> {
                                    try {
                                        Tracing.restoreTracingContext(req.tracer(), span);
                                        logError(span, throwable, "error in getWriteLocation()");
                                        LOG.error(String.format("getWriteLocation()"), throwable);
                                        res.status(500).send(throwable);
                                        writeRequestFailedCount.inc();
                                        return null;
                                    } finally {
                                        span.finish();
                                    }
                                });

                    } else {
                        res.status(200).send(WriteLocationResponse.newBuilder()
                                .setAccessAllowed(false)
                                .build());
                        writeRequestDeniedCount.inc();
                        span.finish();
                    }
                } catch (RuntimeException | Error e) {
                    logError(span, e, "unexpected error");
                    LOG.error("unexpected error", e);
                    res.status(500).send(e);
                }
            });

        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }

    <R> void writeRequest(ServerRequest req, ServerResponse res, DatasetMeta datasetMeta, Span span, String bearerToken, String userId, Consumer<AccessCheckResponse> consumer) {

        AccessCheckRequest accessCheckRequest = AccessCheckRequest.newBuilder()
                .setUserId(userId)
                .setValuation(datasetMeta.getValuation().name())
                .setState(datasetMeta.getState().name())
                .setPath(datasetMeta.getId().getPath())
                .setPrivilege(Privilege.CREATE.name())
                .build();

        userAccessClient.hasAccess(accessCheckRequest, bearerToken).subscribe(
                consumer,
                throwable -> {
                    try {
                        Tracing.restoreTracingContext(req.tracer(), span);
                        logError(span, throwable, "error while performing authServiceFutureStub.hasAccess");
                        LOG.error("writeRequest: error while performing authServiceFutureStub.hasAccess", throwable);
                        LOG.info("Access check request: " + ProtobufJsonUtils.toString(accessCheckRequest));
                        res.status(500).send(throwable);
                        writeRequestFailedCount.inc();
                    } finally {
                        span.finish();
                    }
                }
        );
    }
}
