package no.ssb.dapla.data.access;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import no.ssb.dapla.data.access.health.Health;
import no.ssb.dapla.data.access.metadata.MetadataSigner;
import no.ssb.dapla.data.access.service.CatalogClient;
import no.ssb.dapla.data.access.service.DataAccessHttpService;
import no.ssb.dapla.data.access.service.DataAccessService;
import no.ssb.dapla.data.access.service.GoogleDataAccessService;
import no.ssb.dapla.data.access.service.UserAccessClient;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
                .thenAccept(app -> LOG.info("Webserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    DataAccessApplication(Config config, Tracer tracer, UserAccessClient userAccessClient, CatalogClient catalogClient) {
        put(Config.class, config);

        DataAccessService dataAccessService;
        if (config.get("data-access.provider").exists()) {
            final String className = config.get("data-access.provider").asString().get();
            try {
                dataAccessService = (DataAccessService) Class.forName(className)
                        .getDeclaredConstructor(Config.class)
                        .newInstance(loadConfig(config.get("routing.file").asString().get()));
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not instantiate " + className);
            }
        } else {
            dataAccessService = new GoogleDataAccessService(loadConfig(config.get("routing.file").asString().get()));
        }

        Config signerConfig = config.get("metadatads");
        String keystoreFormat = signerConfig.get("format").asString().get();
        String keystore = signerConfig.get("keystore").asString().get();
        String keyAlias = signerConfig.get("keyAlias").asString().get();
        char[] password = signerConfig.get("password-file").asString()
                .filter(s -> !s.isBlank())
                .map(passwordFile -> Path.of(passwordFile))
                .filter(Files::exists)
                .map(this::readPasswordFromFile)
                .orElseGet(() -> signerConfig.get("password").asString().get().toCharArray());
        String algorithm = signerConfig.get("algorithm").asString().get();

        MetadataSigner metadataSigner = new MetadataSigner(keystoreFormat, keystore, keyAlias, password, algorithm);
        put(MetadataSigner.class, metadataSigner);

        DataAccessHttpService dataAccessHttpService = new DataAccessHttpService(dataAccessService, userAccessClient, catalogClient, metadataSigner);
        put(DataAccessHttpService.class, dataAccessHttpService);

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(MetricsSupport.create())
                .register(Health.create(config, () -> get(WebServer.class)))
                .register(dataAccessHttpService)
                .build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.builder()
                .config(config.get("webserver"))
                .addMediaSupport(ProtobufJsonSupport.create())
                .tracer(tracer)
                .routing(routing)
                .build();
        put(WebServer.class, webServer);
    }

    private char[] readPasswordFromFile(Path passwordPath) {
        try {
            return Files.readString(passwordPath).toCharArray();
        } catch (IOException e) {
            return null;
        }
    }

    private Config loadConfig(String path) {
        return Config.builder().disableEnvironmentVariablesSource().sources(ConfigSources.file(path)).build();
    }

}
