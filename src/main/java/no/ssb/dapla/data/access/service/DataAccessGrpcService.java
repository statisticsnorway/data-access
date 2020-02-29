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
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.traceOutputMessage;

public class DataAccessGrpcService extends DataAccessServiceGrpc.DataAccessServiceImplBase {

    private static final String DEFAULT_LOCATION = "DATA_ACCESS_SERVICE_DEFAULT_LOCATION";
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

    private Role.Privilege toDataAccessPrivilege(no.ssb.dapla.data.access.protobuf.AccessTokenRequest.Privilege privilege) {
        switch (privilege) {
            case READ:
                return Role.Privilege.READ;
            case WRITE:
                return Role.Privilege.CREATE; // TODO: potentially differentiate between create and update
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void getLocation(LocationRequest request, StreamObserver<LocationResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = Tracing.spanFromGrpc(request, "getLocation");
        Span span = tracerAndSpan.span();
        try {
            ListenableFuture<GetDatasetResponse> responseListenableFuture = catalogServiceFutureStub
                    .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(AuthorizationInterceptor.token()))
                    .get(GetDatasetRequest.newBuilder()
                    .setPath(request.getPath())
                    .setTimestamp(request.getSnapshot())
                    .build());
            Futures.addCallback(responseListenableFuture, new FutureCallback<>() {

                @Override
                public void onSuccess(@Nullable GetDatasetResponse getDatasetResponse) {
                    Dataset dataset = getDatasetResponse.getDataset();
                    if (dataset.hasId()) {
                        responseObserver.onNext(LocationResponse.newBuilder()
                                .setParentUri(dataset.getParentUri())
                                .setVersion(Long.toString(getDatasetResponse.getDataset().getId().getTimestamp()))
                                .build());
                    } else {
                        responseObserver.onNext(LocationResponse.newBuilder()
                                //TODO: Implement routing based on valuation and state
                                .setParentUri(System.getenv().get(DEFAULT_LOCATION))
                                .build());

                    }
                    responseObserver.onCompleted();
                    span.finish();
                }

                @Override
                public void onFailure(Throwable throwable) {
                    try {
                        Tracing.restoreTracingContext(tracerAndSpan);
                        logError(span, throwable, "error while preforming catalog get");
                        LOG.error("getAccessToken: error while preforming catalog get", throwable);
                        responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                    } finally {
                        span.finish();
                    }
                }
            }, MoreExecutors.directExecutor());

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
            String userId = request.getUserId();
            ListenableFuture<GetDatasetResponse> responseListenableFuture = catalogServiceFutureStub
                    .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(AuthorizationInterceptor.token()))
                    .get(GetDatasetRequest.newBuilder()
                    .setPath(request.getPath())
                    .build());
            Futures.addCallback(responseListenableFuture, new FutureCallback<>() {

                @Override
                public void onSuccess(@Nullable GetDatasetResponse getDatasetResponse) {
                    Dataset dataset = getDatasetResponse.getDataset();
                    ListenableFuture<AccessCheckResponse> accessCheckResponseListenableFuture = authServiceFutureStub
                            .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(AuthorizationInterceptor.token()))
                            .hasAccess(AccessCheckRequest.newBuilder()
                            .setUserId(userId)
                            .setValuation(dataset.getValuation().name())
                            .setState(dataset.getState().name())
                            .setNamespace(dataset.getId().getPath())
                            .setPrivilege(toDataAccessPrivilege(request.getPrivilege()).name())
                            .build());

                    Futures.addCallback(accessCheckResponseListenableFuture, new FutureCallback<>() {
                        @Override
                        public void onSuccess(@Nullable AccessCheckResponse accessCheckResponse) {
                            if (!accessCheckResponse.getAllowed()) {
                                try {
                                    Tracing.restoreTracingContext(tracerAndSpan);
                                    responseObserver.onError(new StatusException(Status.PERMISSION_DENIED));
                                } finally {
                                    span.finish();
                                }
                                return;
                            }

                            dataAccessService.getAccessToken(span, userId, request.getPrivilege(), request.getPath())
                                    .orTimeout(10, TimeUnit.SECONDS)
                                    .thenAccept(token -> {
                                        Tracing.restoreTracingContext(tracerAndSpan);
                                        responseObserver.onNext(traceOutputMessage(span, AccessTokenResponse.newBuilder()
                                                .setAccessToken(token.getAccessToken()).setExpirationTime(token.getExpirationTime())
                                                .setParentUri(dataset.getParentUri())
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

                        @Override
                        public void onFailure(Throwable throwable) {
                            try {
                                Tracing.restoreTracingContext(tracerAndSpan);
                                logError(span, throwable, "error while preforming authServiceFutureStub.hasAccess");
                                LOG.error("getAccessToken: error while preforming authServiceFutureStub.hasAccess", throwable);
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
                        logError(span, throwable, "error while preforming catalog get");
                        LOG.error("getAccessToken: error while preforming catalog get", throwable);
                        responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                    } finally {
                        span.finish();
                    }
                }
            }, MoreExecutors.directExecutor());

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
}
