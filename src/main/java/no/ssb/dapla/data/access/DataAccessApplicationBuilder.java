package no.ssb.dapla.data.access;

import io.helidon.config.Config;
import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import no.ssb.dapla.data.access.service.CatalogClient;
import no.ssb.dapla.data.access.service.CatalogWebClient;
import no.ssb.dapla.data.access.service.UserAccessClient;
import no.ssb.dapla.data.access.service.UserAccessWebClient;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;

import static java.util.Optional.ofNullable;

public class DataAccessApplicationBuilder extends DefaultHelidonApplicationBuilder {

    UserAccessClient userAccessClient;
    CatalogClient catalogClient;

    @Override
    public HelidonApplicationBuilder override(Class<?> clazz, Object instance) {
        if (UserAccessClient.class.isAssignableFrom(clazz)) {
            this.userAccessClient = (UserAccessClient) instance;
        }
        if (CatalogClient.class.isAssignableFrom(clazz)) {
            this.catalogClient = (CatalogClient) instance;
        }
        return super.override(clazz, instance);
    }

    @Override
    public HelidonApplication build() {
        Config config = ofNullable(this.config).orElseGet(DefaultHelidonApplicationBuilder::createDefaultConfig);

        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(false);
        Tracer tracer = tracerBuilder.build();

        if (catalogClient == null) {
            String host = config.get("catalog-service").get("host").asString().orElseThrow(() ->
                    new RuntimeException("missing configuration: catalog-service.host"));
            int port = config.get("catalog-service").get("port").asInt().orElseThrow(() ->
                    new RuntimeException("missing configuration: catalog-service.port"));
            catalogClient = new CatalogWebClient(host, port);
        }

        if (userAccessClient == null) {
            String host = config.get("auth-service").get("host").asString().orElseThrow(() ->
                    new RuntimeException("missing configuration: auth-service.host"));
            int port = config.get("auth-service").get("port").asInt().orElseThrow(() ->
                    new RuntimeException("missing configuration: auth-service.port"));
            userAccessClient = new UserAccessWebClient(host, port);
        }

        return new DataAccessApplication(config, tracer, userAccessClient, catalogClient);
    }
}
