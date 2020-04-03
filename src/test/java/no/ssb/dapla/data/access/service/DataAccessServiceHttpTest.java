package no.ssb.dapla.data.access.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(IntegrationTestExtension.class)
@GrpcMockRegistryConfig(DataAccessGrpcMockRegistry.class)
class DataAccessServiceHttpTest {

    @Inject
    TestClient testClient;

    String[] headers = new String[]{"Authorization", "Bearer " + JWT.create().withClaim("preferred_username", "user")
            .sign(Algorithm.HMAC256("secret"))};

    @Test
    public void thatReadLocationWorks() {

        ReadLocationResponse response = testClient
                .post("/rpc/DataAccessService/readLocation", ReadLocationRequest.newBuilder()
                                .setPath("/path/to/dataset")
                                .setSnapshot(2)
                                .build(),
                        ReadLocationResponse.class, headers).body();
        assertNotNull(response);
        assertThat(response.getParentUri()).isEqualTo("gs://dev-datalager-store");
        assertThat(response.getVersion()).isEqualTo("1");
    }

    @Test
    public void thatReadAccessTokenWorks() {
        ReadAccessTokenResponse response = testClient.post("/rpc/DataAccessService/readAccessToken", ReadAccessTokenRequest.newBuilder()
                        .setPath("/path/to/dataset")
                        .build(),
                ReadAccessTokenResponse.class, headers).body();
        assertNotNull(response);
        assertThat(response.getAccessToken()).isEqualTo("dev-read.json-read-token");
        assertThat(response.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void thatWriteLocationThenGetAccessTokenWorks() {
        WriteLocationResponse writeLocationResponse = testClient.post("/rpc/DataAccessService/writeLocation", WriteLocationRequest.newBuilder()
                        .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                                .setId(DatasetId.newBuilder()
                                        .setPath("/junit/write-loc-and-access-test")
                                        .setVersion(String.valueOf(System.currentTimeMillis()))
                                        .build())
                                .setType(Type.BOUNDED)
                                .setValuation(Valuation.INTERNAL)
                                .setState(DatasetState.INPUT)
                                .build()))
                        .build(),
                WriteLocationResponse.class, headers).body();

        assertNotNull(writeLocationResponse);
        assertThat(writeLocationResponse.getAccessAllowed()).isTrue();
        DatasetMeta signedDatasetMeta = ProtobufJsonUtils.toPojo(writeLocationResponse.getValidMetadataJson().toStringUtf8(), DatasetMeta.class);
        assertThat(signedDatasetMeta.getCreatedBy()).isEqualTo("user");

        WriteAccessTokenResponse writeAccessTokenResponse = testClient.post("/rpc/DataAccessService/writeAccessToken", WriteAccessTokenRequest.newBuilder()
                        .setMetadataJson(writeLocationResponse.getValidMetadataJson())
                        .setMetadataSignature(writeLocationResponse.getMetadataSignature())
                        .build(),
                WriteAccessTokenResponse.class, headers).body();

        assertThat(writeAccessTokenResponse.getAccessToken()).isEqualTo("dev-datalager-store-write-token");
        assertThat(writeAccessTokenResponse.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }
}
