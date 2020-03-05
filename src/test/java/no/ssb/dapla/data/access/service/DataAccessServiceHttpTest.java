package no.ssb.dapla.data.access.service;

import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.data.access.protobuf.Privilege;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(IntegrationTestExtension.class)
@GrpcMockRegistryConfig(DataAccessServiceGrpcTest.DataAccessGrpcMockRegistry.class)
class DataAccessServiceHttpTest {

    @Inject
    TestClient testClient;

    @Test
    void thatGetAccessTokenWorks() {
        AccessTokenRequest accessTokenRequest = AccessTokenRequest.newBuilder()
                .setPath("/path/to/dataset")
                .setPrivilege(Privilege.READ)
                .build();
        AccessTokenResponse response = testClient.post("/rpc/DataAccessService/getAccessToken", accessTokenRequest,
                AccessTokenResponse.class).body();
        assertNotNull(response);
        assertThat(response.getAccessToken()).isEqualTo("localstack-token");
        assertThat(response.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void thatInvalidUserFails() {
        AccessTokenRequest accessTokenRequest = AccessTokenRequest.newBuilder()
                .setPath("/path/to/dataset")
                .setPrivilege(Privilege.READ)
                .build();

        testClient.post("/rpc/DataAccessService/getAccessToken", accessTokenRequest).expect403Forbidden();
    }

    @Test
    public void thatGetLocationWorks() {
        LocationRequest locationRequest = LocationRequest.newBuilder()
                .setSnapshot(2)
                .setPath("/path/to/dataset")
                .build();

        LocationResponse response = testClient.post("/rpc/DataAccessService/getLocation", locationRequest,
                LocationResponse.class).body();
        assertNotNull(response);
        assertThat(response.getParentUri()).isEqualTo("gs://root");
        assertThat(response.getVersion()).isEqualTo("1");
    }

}
