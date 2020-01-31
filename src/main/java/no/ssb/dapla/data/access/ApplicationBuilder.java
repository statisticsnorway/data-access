package no.ssb.dapla.data.access;

import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;

import static java.util.Optional.ofNullable;

public class ApplicationBuilder extends DefaultHelidonApplicationBuilder {

    @Override
    public Application build() {
        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(true);
        Tracer tracer = tracerBuilder.build();
        return new Application(ofNullable(config).orElseGet(() -> createDefaultConfig()), tracer);
    }
}
