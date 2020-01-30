package no.ssb.dapla.data.access.service;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.helidon.application.Tracing;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.traceOutputMessage;

public class DataAccessGrpcService extends DataAccessServiceGrpc.DataAccessServiceImplBase {

    private final DataAccessService dataAccessService;
    private static final Logger LOG = LoggerFactory.getLogger(DataAccessGrpcService.class);

    public DataAccessGrpcService(DataAccessService dataAccessService) {
        this.dataAccessService = dataAccessService;
    }

    @Override
    public void getAccessToken(AccessTokenRequest request, StreamObserver<AccessTokenResponse> responseObserver) {
        Span span = Tracing.spanFromGrpc(request, "getAccessToken");
        try {
            String userId = request.getUserId();
            dataAccessService.getAccessToken(span, userId, request.getPrivilege(), request.getLocation())
                    .orTimeout(10, TimeUnit.SECONDS)
                    .thenAccept(token -> {
                        responseObserver.onNext(traceOutputMessage(span, AccessTokenResponse.newBuilder()
                                .setAccessToken(token.getAccessToken()).setExpirationTime(token.getExpirationTime())
                                .build()));
                        responseObserver.onCompleted();
                        span.finish();
                    })
                    .exceptionally(throwable -> {
                        try {
                            logError(span, throwable, "error in getAccessToken()");
                            LOG.error(String.format("getAccessToken()"), throwable);
                            responseObserver.onError(new StatusException(Status.fromThrowable(throwable)));
                            return null;
                        } finally {
                            span.finish();
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
    public void getLocation(LocationRequest request, StreamObserver<LocationResponse> responseObserver) {
        super.getLocation(request, responseObserver);
    }
}
