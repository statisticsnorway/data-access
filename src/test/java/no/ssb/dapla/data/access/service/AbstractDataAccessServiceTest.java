package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import no.ssb.dapla.dataset.api.DatasetMeta;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

public class AbstractDataAccessServiceTest {

    private static final String ROUTE = "src/test/resources/routing.json";

    private AbstractDataAccessService sut = new LocalstackDataAccessService(
            Config.builder().sources(ConfigSources.file(ROUTE)).build());

    @Test
    void testSensitivePath() {
        Route route = sut.getRoute("/raw/skatt/sensitive", DatasetMeta.Valuation.SENSITIVE, DatasetMeta.DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("sensitive-rawdata-skatt-read.json");
    }

    @Test
    void testNotSensitivePath() {
        Route route = sut.getRoute("/raw/skatt/shielded", DatasetMeta.Valuation.SHIELDED, DatasetMeta.DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("not-so-sensitive-rawdata-skatt-read.json");
    }

    @Test
    void testExludeWeirdPath() {
        Route route = sut.getRoute("/raw/skatt/weird-special", DatasetMeta.Valuation.SENSITIVE, DatasetMeta.DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("catch-all-read.json");
    }

    @Test
    void testTempPath() {
        Route route = sut.getRoute("/tmp/gunnar", DatasetMeta.Valuation.SENSITIVE, DatasetMeta.DatasetState.RAW);
        assertThat(route.getAuth().get("write")).isEqualTo("tmp-write.json");
    }

    @Test
    void testCatchAllPath() {
        Route route = sut.getRoute("/catch-me-please", DatasetMeta.Valuation.SENSITIVE, DatasetMeta.DatasetState.RAW);
        assertThat(route.getAuth().get("read")).isEqualTo("catch-all-read.json");
    }

    @Test
    void testGetTokenByExistingRoute() {
        Route route = sut.getRoute("gs", "dev-datalager-store");
        assertThat(route.getAuth().get("read")).isEqualTo("dev-read.json");
    }


}
