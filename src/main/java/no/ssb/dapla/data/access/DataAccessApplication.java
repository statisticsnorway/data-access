package no.ssb.dapla.data.access;

import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import io.helidon.grpc.server.GrpcServerConfiguration;
import io.helidon.grpc.server.GrpcTracingConfig;
import io.helidon.grpc.server.ServerRequestAttribute;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceFutureStub;
import no.ssb.dapla.data.access.health.Health;
import no.ssb.dapla.data.access.service.DataAccessGrpcService;
import no.ssb.dapla.data.access.service.DataAccessService;
import no.ssb.dapla.data.access.service.GoogleDataAccessService;
import no.ssb.dapla.data.access.service.LocalstackDataAccessService;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.application.HelidonGrpcWebTranscoding;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class DataAccessApplication extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(DataAccessApplication.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new DataAccessApplicationBuilder().build()
                .start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    DataAccessApplication(Config config, Tracer tracer, AuthServiceFutureStub authServiceGrpc, CatalogServiceFutureStub catalogServiceGrpc) {
        put(Config.class, config);

        DataAccessService dataAccessService;
        if (config.get("data-access.provider").asString().equals(GoogleDataAccessService.class.getName())) {
            dataAccessService = new GoogleDataAccessService();
        } else {
            dataAccessService = new LocalstackDataAccessService();
        }

        DataAccessGrpcService dataAccessGrpcService = new DataAccessGrpcService(dataAccessService, authServiceGrpc, catalogServiceGrpc);
        put(DataAccessGrpcService.class, dataAccessGrpcService);

        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.builder(config.get("grpcserver"))
                        .tracer(tracer)
                        .tracingConfig(GrpcTracingConfig.builder()
                                .withStreaming()
                                .withVerbosity()
                                .withOperationName(new OperationNameConstructor() {
                                    @Override
                                    public <ReqT, RespT> String constructOperationName(MethodDescriptor<ReqT, RespT> method) {
                                        return "Grpc server received " + method.getFullMethodName();
                                    }
                                })
                                .withTracedAttributes(ServerRequestAttribute.CALL_ATTRIBUTES,
                                        ServerRequestAttribute.HEADERS,
                                        ServerRequestAttribute.METHOD_NAME)
                                .build()
                        ),
                GrpcRouting.builder()
                        .intercept(new AuthorizationInterceptor())
                        .register(dataAccessGrpcService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);


        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register(Health.create(config, () -> get(WebServer.class)))
                .register("/rpc", new HelidonGrpcWebTranscoding(
                        () -> ManagedChannelBuilder
                                .forAddress("localhost", Optional.of(grpcServer)
                                        .filter(GrpcServer::isRunning)
                                        .map(GrpcServer::port)
                                        .orElseThrow())
                                .usePlaintext()
                                .build(),
                        dataAccessGrpcService
                )).build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

}
