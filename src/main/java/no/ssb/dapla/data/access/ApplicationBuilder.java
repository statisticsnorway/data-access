package no.ssb.dapla.data.access;

import io.helidon.config.Config;
import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;

import static java.util.Optional.ofNullable;

public class ApplicationBuilder extends DefaultHelidonApplicationBuilder {

    @Override
    public Application build() {
        Config config = ofNullable(this.config).orElseGet(() -> createDefaultConfig());
        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(true);
        Tracer tracer = tracerBuilder.build();
        return new Application(config, tracer);
    }
}
