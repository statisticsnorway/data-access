package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import no.ssb.dapla.dataset.api.DatasetMeta;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


public class StagingAccessTest {

    private static final String ROUTE = "src/test/resources/routing/staging.json";

    private AbstractDataAccessService sut = new LocalstackDataAccessService(
            Config.builder().sources(ConfigSources.file(ROUTE)).build());

    @Test
    void testRawPath() {
        Route route = sut.getRoute("/raw/skatt/test", DatasetMeta.Valuation.SENSITIVE, DatasetMeta.DatasetState.RAW);
        assertThat(route.getUri().getHost()).isEqualTo("staging-rawdata-store");
        assertThat(route.getUri().getPath()).isEqualTo("/datastore");
    }

    @Test
    void testSensitivePath() {
        Route route = sut.getRoute("/skatt/test-sensitive", DatasetMeta.Valuation.SENSITIVE, DatasetMeta.DatasetState.INPUT);
        assertThat(route.getUri().getHost()).isEqualTo("ssb-data-staging");
        assertThat(route.getUri().getPath()).isEqualTo("/datastore/sensitive");
    }

    @Test
    void testNotSensitivePath() {
        Route route = sut.getRoute("/skatt/test-shielded", DatasetMeta.Valuation.SHIELDED, DatasetMeta.DatasetState.INPUT);
        assertThat(route.getUri().getHost()).isEqualTo("ssb-data-staging");
        assertThat(route.getUri().getPath()).isEqualTo("/datastore/work");
    }

    @Test
    void testTempPath() {
        Route route = sut.getRoute("/tmp/gunnar", DatasetMeta.Valuation.INTERNAL, DatasetMeta.DatasetState.OUTPUT);
        assertThat(route.getUri().getHost()).isEqualTo("ssb-data-staging");
        assertThat(route.getUri().getPath()).isEqualTo("/datastore/tmp");
    }

    @Test
    void testCatchAllPath() {
        Route route = sut.getRoute("/catch-me-please", DatasetMeta.Valuation.INTERNAL, DatasetMeta.DatasetState.OTHER);
        assertThat(route.getUri().getHost()).isEqualTo("ssb-data-staging");
        assertThat(route.getUri().getPath()).isEqualTo("/datastore/other");
    }

    @Test
    void testGetTokenByExistingRoute() {
        Route route = sut.getRoute("gs", "ssb-data-staging");
        assertThat(route.getAuth().get("read")).isEqualTo("/gcloud/staging-bip-ssb-data-store.json");
    }


}
