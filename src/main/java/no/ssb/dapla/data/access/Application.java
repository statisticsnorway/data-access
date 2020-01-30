package no.ssb.dapla.data.access;

import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import no.ssb.dapla.data.access.health.Health;
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
import io.opentracing.contrib.grpc.OperationNameConstructor;
import no.ssb.dapla.data.access.service.DataAccessGrpcService;
import no.ssb.dapla.data.access.service.DataAccessHttpService;
import no.ssb.dapla.data.access.service.DataAccessService;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Application extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new ApplicationBuilder().build()
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

    Application(Config config) {
        put(Config.class, config);

        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(true);
        Tracer tracer = tracerBuilder.build();

        DataAccessService dataAccessService = new DataAccessService();

        DataAccessGrpcService dataAccessGrpcService = new DataAccessGrpcService(dataAccessService);
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

        DataAccessHttpService dataAccessHttpService = new DataAccessHttpService(dataAccessService);
        put(DataAccessHttpService.class, dataAccessHttpService);

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register(Health.create(config, () -> get(WebServer.class)))
                .register("access", dataAccessHttpService)
                .build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

}
