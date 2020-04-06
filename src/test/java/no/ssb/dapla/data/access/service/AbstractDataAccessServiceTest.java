package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Valuation;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AbstractDataAccessServiceTest {

    private static final String ROUTE = "src/test/resources/routing.json";

    private AbstractDataAccessService sut = new MockDataAccessService(
            Config.builder().sources(ConfigSources.file(ROUTE)).build());

    @Test
    void testSensitivePath() {
        Route route = sut.getRoute("/raw/skatt/sensitive", Valuation.SENSITIVE, DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("sensitive-rawdata-skatt-read.json");
    }

    @Test
    void testNotSensitivePath() {
        Route route = sut.getRoute("/raw/skatt/shielded", Valuation.SHIELDED, DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("not-so-sensitive-rawdata-skatt-read.json");
    }

    @Test
    void testExludeWeirdPath() {
        Route route = sut.getRoute("/raw/skatt/weird-special", Valuation.SENSITIVE, DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("catch-all-read.json");
    }

    @Test
    void testTempPath() {
        Route route = sut.getRoute("/tmp/gunnar", Valuation.SENSITIVE, DatasetState.RAW);
        // Empty auth should be allowed (in reality only used for gcs)
        assertThat(route.getAuth()).isEmpty();
        assertThat(route.getUri().toString()).isEqualTo("file:///data/datastore/tmp");
    }

    @Test
    void testCatchAllPath() {
        Route route = sut.getRoute("/catch-me-please", Valuation.SENSITIVE, DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("catch-all-read.json");
    }

    @Test
    void testGetTokenByExistingRoute() {
        Route route = sut.getRoute("gs", "dev-datalager-store");
        assertThat(route.getAuth().get("read")).isEqualTo("dev-read.json");
    }

    @Test
    void testGetTokenByExistingRouteExceptionHandling() {
        NoSuchElementException exception = assertThrows(NoSuchElementException.class,
                () -> sut.getRoute("gs", "not-found"), "Expect access denied exception");
        assertEquals(exception.getMessage(), "Could not find target: gs://not-found");
    }


}
