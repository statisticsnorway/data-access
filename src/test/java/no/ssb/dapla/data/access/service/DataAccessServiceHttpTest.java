package no.ssb.dapla.data.access.service;

import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

@ExtendWith(IntegrationTestExtension.class)
class DataAccessServiceHttpTest {

    @Inject
    TestClient testClient;

    @Test
    void thatGetAccessTokenWorks() {
        testClient.get("/access/location?userId=user&privilege=READ", AccessTokenResponse.class).expect200Ok().body();
    }

}
