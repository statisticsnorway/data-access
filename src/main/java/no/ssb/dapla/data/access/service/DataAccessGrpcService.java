package no.ssb.dapla.data.access.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.data.access.metadata.MetadataSigner;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.traceOutputMessage;

public class DataAccessGrpcService extends DataAccessServiceGrpc.DataAccessServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(DataAccessGrpcService.class);

    private final DataAccessService dataAccessService;
    private final AuthServiceFutureStub authServiceFutureStub;
    private final CatalogServiceFutureStub catalogServiceFutureStub;

    private final MetadataSigner metadataSigner;

    public DataAccessGrpcService(DataAccessService dataAccessService,
                                 AuthServiceFutureStub authServiceFutureStub,
                                 CatalogServiceFutureStub catalogServiceFutureStub, MetadataSigner metadataSigner) {
        this.dataAccessService = dataAccessService;
        this.authServiceFutureStub = authServiceFutureStub;
        this.catalogServiceFutureStub = catalogServiceFutureStub;
        this.metadataSigner = metadataSigner;
    }

    @Override
    public void readLocation(ReadLocationRequest request, StreamObserver<ReadLocationResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getLocation");
        Span span = tracerAndSpan.span();
        try {
            String bearerToken = AuthorizationInterceptor.token();
            final GrpcAuthorizationBearerCallCredentials credentials = new GrpcAuthorizationBearerCallCredentials(bearerToken);
            DecodedJWT decodedJWT = JWT.decode(bearerToken);
            String userId = decodedJWT.getClaim("preferred_username").asString();
            //String userId = decodedJWT.getSubject(); // TODO use subject instead of preferred_username

            readRequest(responseObserver, request.getPath(), String.valueOf(request.getSnapshot()), tracerAndSpan, span, credentials, userId,
                    (getDatasetResponse, accessCheckResponse) -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            ReadLocationResponse response = ReadLocationResponse.newBuilder()
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
                                            .orElse(""))
                                    .build();
                            responseObserver.onNext(traceOutputMessage(span, response));
                            responseObserver.onCompleted();
                            span.finish();
                        } catch (RuntimeException | Error e) {
                            logError(span, e, "unexpected error");
                            LOG.error("unexpected error", e);
                            responseObserver.onError(e);
                        }
                    }
            );

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

    @Override
    public void readAccessToken(ReadAccessTokenRequest request, StreamObserver<ReadAccessTokenResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getLocation");
        Span span = tracerAndSpan.span();
        try {
            String bearerToken = AuthorizationInterceptor.token();
            final GrpcAuthorizationBearerCallCredentials credentials = new GrpcAuthorizationBearerCallCredentials(bearerToken);
            DecodedJWT decodedJWT = JWT.decode(bearerToken);
            String userId = decodedJWT.getClaim("preferred_username").asString();
            //String userId = decodedJWT.getSubject(); // TODO use subject instead of preferred_username

            readRequest(responseObserver, request.getPath(), request.getVersion(), tracerAndSpan, span, credentials, userId,
                    (getDatasetResponse, accessCheckResponse) -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);

                            if (!accessCheckResponse.getAllowed()) {
                                try {
                                    responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
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
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        responseObserver.onNext(traceOutputMessage(span, ReadAccessTokenResponse.newBuilder()
                                                .setAccessToken(token.getAccessToken())
                                                .setExpirationTime(token.getExpirationTime())
                                                .setParentUri(getDatasetResponse.getDataset().getParentUri())
                                                .setVersion(String.valueOf(getDatasetResponse.getDataset().getId().getTimestamp()))
                                                .build()));
                                        responseObserver.onCompleted();
                                        span.finish();
                                    })
                                    .exceptionally(throwable -> {
                                        try {
                                            Tracing.restoreTracingContext(tracerAndSpan);
                                            logError(span, throwable, "error in getAccessToken()");
                                            LOG.error(String.format("getAccessToken()"), throwable);
                                            responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                                            return null;
                                        } finally {
                                            span.finish();
                                        }
                                    });
                        } catch (RuntimeException | Error e) {
                            logError(span, e, "unexpected error");
                            LOG.error("unexpected error", e);
                            responseObserver.onError(e);
                        }
                    }
            );

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

    <R> void readRequest(StreamObserver<R> responseObserver, final String path, String version, TracerAndSpan tracerAndSpan, Span span, GrpcAuthorizationBearerCallCredentials credentials, String userId, BiConsumer<GetDatasetResponse, AccessCheckResponse> onUserAccessResponseConsumer) {

        // TODO Add fallback that reads metadata directly from bucket instead of catalog. That will allow this service
        // TODO to continue functioning even when catalog is down.

        ListenableFuture<GetDatasetResponse> responseListenableFuture = catalogServiceFutureStub
                .withCallCredentials(credentials)
                .get(GetDatasetRequest.newBuilder()
                        .setPath(path)
                        .setTimestamp((version == null || version.length() == 0) ? 0 : Long.parseLong(version))
                        .build());

        Futures.addCallback(responseListenableFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable GetDatasetResponse getDatasetResponse) {

                if (ofNullable(getDatasetResponse)
                        .filter(GetDatasetResponse::hasDataset)
                        .map(GetDatasetResponse::getDataset)
                        .map(Dataset::getId)
                        .map(DatasetId::getPath)
                        .orElse("")
                        .isBlank()) {
                    // no record of dataset in catalog
                    responseObserver.onError(new StatusException(Status.NOT_FOUND));
                    return;
                }

                Dataset dataset = getDatasetResponse.getDataset();
                AccessCheckRequest accessCheckRequest = AccessCheckRequest.newBuilder()
                        .setUserId(userId)
                        .setValuation(dataset.getValuation().name())
                        .setState(dataset.getState().name())
                        .setNamespace(path)
                        .setPrivilege(Role.Privilege.READ.name())
                        .build();

                ListenableFuture<AccessCheckResponse> accessCheckResponseListenableFuture = authServiceFutureStub
                        .withCallCredentials(credentials)
                        .hasAccess(accessCheckRequest);

                Futures.addCallback(accessCheckResponseListenableFuture, new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable AccessCheckResponse accessCheckResponse) {
                        onUserAccessResponseConsumer.accept(getDatasetResponse, accessCheckResponse);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            logError(span, throwable, "error while performing authServiceFutureStub.hasAccess");
                            LOG.error("getAccessToken: error while performing authServiceFutureStub.hasAccess", throwable);
                            LOG.info("Access check request: " + ProtobufJsonUtils.toString(accessCheckRequest));
                            responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                        } finally {
                            span.finish();
                        }
                    }
                }, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable throwable) {
                try {
                    Tracing.restoreTracingContext(tracerAndSpan);
                    logError(span, throwable, "error while performing catalog get");
                    LOG.error("readRequest: error while performing catalog get", throwable);
                    responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                } finally {
                    span.finish();
                }
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void writeLocation(WriteLocationRequest request, StreamObserver<WriteLocationResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getLocation");
        Span span = tracerAndSpan.span();
        try {
            String bearerToken = AuthorizationInterceptor.token();
            final GrpcAuthorizationBearerCallCredentials credentials = new GrpcAuthorizationBearerCallCredentials(bearerToken);
            DecodedJWT decodedJWT = JWT.decode(bearerToken);
            String userId = decodedJWT.getClaim("preferred_username").asString();
            //String userId = decodedJWT.getSubject(); // TODO use subject instead of preferred_username

            DatasetMeta untrustedMetadata = ProtobufJsonUtils.toPojo(request.getMetadataJson(), DatasetMeta.class);

            writeRequest(responseObserver, untrustedMetadata, tracerAndSpan, span, credentials, userId,
                    accessCheckResponse -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            if (accessCheckResponse.getAllowed()) {

                                CompletableFuture<URI> accessTokenFuture = dataAccessService.getWriteLocation(
                                        span, userId, untrustedMetadata.getId().getPath(), untrustedMetadata.getValuation(), untrustedMetadata.getState()
                                );

                                accessTokenFuture
                                        .orTimeout(10, TimeUnit.SECONDS)
                                        .thenAccept(location -> {
                                            Tracing.restoreTracingContext(tracerAndSpan);
                                            DatasetMeta allowedMetadata = DatasetMeta.newBuilder()
                                                    .mergeFrom(untrustedMetadata)
                                                    .setCreatedBy(userId)
                                                    .build();

                                            ByteString validMetadataJson = ByteString.copyFromUtf8(ProtobufJsonUtils.toString(allowedMetadata));
                                            ByteString signature = ByteString.copyFrom(metadataSigner.sign(validMetadataJson.toByteArray()));

                                            responseObserver.onNext(traceOutputMessage(span, WriteLocationResponse.newBuilder()
                                                    .setAccessAllowed(true)
                                                    .setValidMetadataJson(validMetadataJson)
                                                    .setMetadataSignature(signature)
                                                    .setParentUri(location.toString())
                                                    .build()));
                                        })
                                        .exceptionally(throwable -> {
                                            try {
                                                Tracing.restoreTracingContext(tracerAndSpan);
                                                logError(span, throwable, "error in getAccessToken()");
                                                LOG.error(String.format("getAccessToken()"), throwable);
                                                responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                                                return null;
                                            } finally {
                                                span.finish();
                                            }
                                        });

                            } else {
                                responseObserver.onNext(traceOutputMessage(span, WriteLocationResponse.newBuilder()
                                        .setAccessAllowed(false)
                                        .build()));
                            }
                            responseObserver.onCompleted();
                            span.finish();
                        } catch (RuntimeException | Error e) {
                            logError(span, e, "unexpected error");
                            LOG.error("unexpected error", e);
                            responseObserver.onError(e);
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

    @Override
    public void writeAccessToken(WriteAccessTokenRequest request, StreamObserver<WriteAccessTokenResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getLocation");
        Span span = tracerAndSpan.span();
        try {
            String bearerToken = AuthorizationInterceptor.token();
            final GrpcAuthorizationBearerCallCredentials credentials = new GrpcAuthorizationBearerCallCredentials(bearerToken);
            DecodedJWT decodedJWT = JWT.decode(bearerToken);
            String userId = decodedJWT.getClaim("preferred_username").asString();
            //String userId = decodedJWT.getSubject(); // TODO use subject instead of preferred_username

            boolean valid = metadataSigner.verify(request.getMetadataJson().toByteArray(), request.getMetadataSignature().toByteArray());
            if (!valid) {
                try {
                    span.log("invalid metadata signature");
                    LOG.error("invalid metadata signature");
                    responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                    return;
                } finally {
                    span.finish();
                }
            }

            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(request.getMetadataJson().toStringUtf8(), DatasetMeta.class);

            writeRequest(responseObserver, datasetMeta, tracerAndSpan, span, credentials, userId,
                    accessCheckResponse -> {
                        try {
                            Tracing.restoreTracingContext(tracerAndSpan);

                            if (!accessCheckResponse.getAllowed()) {
                                try {
                                    responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
                                } finally {
                                    span.finish();
                                }
                                return;
                            }

                            CompletableFuture<AccessToken> accessTokenFuture = dataAccessService.getWriteAccessToken(
                                    span, userId, datasetMeta.getId().getPath(), datasetMeta.getValuation(), datasetMeta.getState()
                            );

                            accessTokenFuture
                                    .orTimeout(10, TimeUnit.SECONDS)
                                    .thenAccept(token -> {
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        responseObserver.onNext(traceOutputMessage(span, WriteAccessTokenResponse.newBuilder()
                                                .setAccessToken(token.getAccessToken())
                                                .setExpirationTime(token.getExpirationTime())
                                                .build()));
                                        responseObserver.onCompleted();
                                        span.finish();
                                    })
                                    .exceptionally(throwable -> {
                                        try {
                                            Tracing.restoreTracingContext(tracerAndSpan);
                                            logError(span, throwable, "error in getAccessToken()");
                                            LOG.error(String.format("getAccessToken()"), throwable);
                                            responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                                            return null;
                                        } finally {
                                            span.finish();
                                        }
                                    });
                        } catch (RuntimeException | Error e) {
                            logError(span, e, "unexpected error");
                            LOG.error("unexpected error", e);
                            responseObserver.onError(e);
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

    <R> void writeRequest(StreamObserver<R> responseObserver, DatasetMeta datasetMeta, TracerAndSpan tracerAndSpan, Span span, GrpcAuthorizationBearerCallCredentials credentials, String userId, Consumer<AccessCheckResponse> consumer) {

        AccessCheckRequest accessCheckRequest = AccessCheckRequest.newBuilder()
                .setUserId(userId)
                .setValuation(datasetMeta.getValuation().name())
                .setState(datasetMeta.getState().name())
                .setNamespace(datasetMeta.getId().getPath())
                .setPrivilege(Role.Privilege.CREATE.name())
                .build();

        ListenableFuture<AccessCheckResponse> accessCheckResponseListenableFuture = authServiceFutureStub
                .withCallCredentials(credentials)
                .hasAccess(accessCheckRequest);

        Futures.addCallback(accessCheckResponseListenableFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable AccessCheckResponse accessCheckResponse) {
                consumer.accept(accessCheckResponse);
            }

            @Override
            public void onFailure(Throwable throwable) {
                try {
                    Tracing.restoreTracingContext(tracerAndSpan);
                    logError(span, throwable, "error while performing authServiceFutureStub.hasAccess");
                    LOG.error("writeRequest: error while performing authServiceFutureStub.hasAccess", throwable);
                    LOG.info("Access check request: " + ProtobufJsonUtils.toString(accessCheckRequest));
                    responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                } finally {
                    span.finish();
                }
            }
        }, MoreExecutors.directExecutor());
    }
}
