package no.ssb.dapla.data.access.service;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.DatasetState;
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.data.access.protobuf.Privilege;
import no.ssb.dapla.data.access.protobuf.Valuation;
import no.ssb.dapla.data.access.protobuf.WriteOptions;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.traceOutputMessage;

public class DataAccessGrpcService extends DataAccessServiceGrpc.DataAccessServiceImplBase {

    private final DataAccessService dataAccessService;
    private final AuthServiceFutureStub authServiceFutureStub;
    private final CatalogServiceFutureStub catalogServiceFutureStub;
    private static final Logger LOG = LoggerFactory.getLogger(DataAccessGrpcService.class);

    public DataAccessGrpcService(DataAccessService dataAccessService,
                                 AuthServiceFutureStub authServiceFutureStub,
                                 CatalogServiceFutureStub catalogServiceFutureStub) {
        this.dataAccessService = dataAccessService;
        this.authServiceFutureStub = authServiceFutureStub;
        this.catalogServiceFutureStub = catalogServiceFutureStub;
    }

    @Override
    public void getLocation(LocationRequest request, StreamObserver<LocationResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getLocation");
        Span span = tracerAndSpan.span();
        try {
            final GrpcAuthorizationBearerCallCredentials credentials = new GrpcAuthorizationBearerCallCredentials(AuthorizationInterceptor.token());
            String userId = request.getUserId();

            if (Privilege.READ.equals(request.getPrivilege())) {

                readRequest(responseObserver, request.getPath(), request.getSnapshot(), tracerAndSpan, span, credentials, userId,
                        (getDatasetResponse, accessCheckResponse) -> {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            LocationResponse locationResponse = LocationResponse.newBuilder()
                                    .setAccessAllowed(accessCheckResponse.getAllowed())
                                    .setParentUri(ofNullable(getDatasetResponse)
                                            .filter(GetDatasetResponse::hasDataset)
                                            .map(GetDatasetResponse::getDataset)
                                            .map(Dataset::getParentUri)
                                            .filter(parentUri -> !parentUri.isBlank())
                                            .orElseThrow()
                                    )
                                    .setVersion(ofNullable(getDatasetResponse)
                                            .filter(GetDatasetResponse::hasDataset)
                                            .map(GetDatasetResponse::getDataset)
                                            .map(Dataset::getId)
                                            .map(DatasetId::getTimestamp)
                                            .filter(ts -> ts != 0)
                                            .map(String::valueOf)
                                            .orElseThrow())
                                    .build();
                            responseObserver.onNext(traceOutputMessage(span, locationResponse));
                            responseObserver.onCompleted();
                            span.finish();
                        }
                );

            } else { // WRITE

                writeRequest(responseObserver, request.hasWriteOptions(), request.getWriteOptions(), request.getPath(), tracerAndSpan, span, credentials, userId,
                        accessCheckResponse -> {
                            Tracing.restoreTracingContext(tracerAndSpan);
                            if (accessCheckResponse.getAllowed()) {
                                responseObserver.onNext(traceOutputMessage(span, LocationResponse.newBuilder()
                                        .setAccessAllowed(true)
                                        .setParentUri(System.getenv().get("DATA_ACCESS_SERVICE_DEFAULT_LOCATION")) //TODO: Implement routing based on path, valuation, and state
                                        .build()));
                            } else {
                                responseObserver.onNext(traceOutputMessage(span, LocationResponse.newBuilder()
                                        .setAccessAllowed(false)
                                        .build()));
                            }
                            responseObserver.onCompleted();
                            span.finish();
                        });
            }
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
    public void getAccessToken(AccessTokenRequest request, StreamObserver<AccessTokenResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getAccessToken");
        Span span = tracerAndSpan.span();
        try {
            final GrpcAuthorizationBearerCallCredentials credentials = new GrpcAuthorizationBearerCallCredentials(AuthorizationInterceptor.token());
            String userId = request.getUserId();

            if (Privilege.READ.equals(request.getPrivilege())) {

                readRequest(responseObserver, request.getPath(), request.getSnapshot(), tracerAndSpan, span, credentials, userId,
                        (getDatasetResponse, accessCheckResponse) -> {
                            Tracing.restoreTracingContext(tracerAndSpan);

                            if (!accessCheckResponse.getAllowed()) {
                                try {
                                    responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
                                } finally {
                                    span.finish();
                                }
                                return;
                            }

                            CompletableFuture<AccessToken> accessTokenFuture = dataAccessService.getAccessToken(
                                    span, userId, request.getPrivilege(), request.getPath(),
                                    Valuation.valueOf(getDatasetResponse.getDataset().getValuation().name()),
                                    DatasetState.valueOf(getDatasetResponse.getDataset().getState().name()));

                            accessTokenFuture
                                    .orTimeout(10, TimeUnit.SECONDS)
                                    .thenAccept(token -> {
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        responseObserver.onNext(traceOutputMessage(span, AccessTokenResponse.newBuilder()
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
                        }
                );

            } else { // WRITE

                writeRequest(responseObserver, request.hasWriteOptions(), request.getWriteOptions(), request.getPath(), tracerAndSpan, span, credentials, userId,
                        accessCheckResponse -> {
                            Tracing.restoreTracingContext(tracerAndSpan);

                            if (!accessCheckResponse.getAllowed()) {
                                try {
                                    responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
                                } finally {
                                    span.finish();
                                }
                                return;
                            }

                            CompletableFuture<AccessToken> accessTokenFuture = dataAccessService.getAccessToken(
                                    span, userId, request.getPrivilege(), request.getPath(),
                                    request.getWriteOptions().getValuation(),
                                    request.getWriteOptions().getState());

                            accessTokenFuture
                                    .orTimeout(10, TimeUnit.SECONDS)
                                    .thenAccept(token -> {
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        responseObserver.onNext(traceOutputMessage(span, AccessTokenResponse.newBuilder()
                                                .setAccessToken(token.getAccessToken())
                                                .setExpirationTime(token.getExpirationTime())
                                                .setParentUri(token.getParentUri())
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
                        });
            }
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

    <R> void readRequest(StreamObserver<R> responseObserver, final String path, long snapshot, TracerAndSpan tracerAndSpan, Span span, GrpcAuthorizationBearerCallCredentials credentials, String userId, BiConsumer<GetDatasetResponse, AccessCheckResponse> onUserAccessResponseConsumer) {

        // TODO Add fallback that reads metadata directly from bucket instead of catalog. That will allow this service
        // TODO to continue functioning even when catalog is down.

        ListenableFuture<GetDatasetResponse> responseListenableFuture = catalogServiceFutureStub
                .withCallCredentials(credentials)
                .get(GetDatasetRequest.newBuilder()
                        .setPath(path)
                        .setTimestamp(snapshot)
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

    <R> void writeRequest(StreamObserver<R> responseObserver, boolean hasWriteOptions, WriteOptions writeOptions, String path, TracerAndSpan tracerAndSpan, Span span, GrpcAuthorizationBearerCallCredentials credentials, String userId, Consumer<AccessCheckResponse> consumer) {

        if (!hasWriteOptions) {
            // write operation without required write-options
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
            return;
        }

        AccessCheckRequest accessCheckRequest = AccessCheckRequest.newBuilder()
                .setUserId(userId)
                .setValuation(writeOptions.getValuation().name())
                .setState(writeOptions.getState().name())
                .setNamespace(path)
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
