package no.ssb.dapla.data.access.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.MockRegistryConfig;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(IntegrationTestExtension.class)
@MockRegistryConfig(DataAccessMockRegistry.class)
class DataAccessServiceHttpTest {

    @Inject
    TestClient testClient;

    String[] headers = new String[]{"Authorization", "Bearer " + JWT.create().withClaim("preferred_username", "user")
            .sign(Algorithm.HMAC256("secret"))};

    @Test
    public void thatReadLocationWorks() {
        ReadLocationResponse response = testClient
                .postAsJson("/rpc/DataAccessService/readLocation", ReadLocationRequest.newBuilder()
                                .setPath("/path/to/dataset")
                                .setSnapshot(2)
                                .build(),
                        ReadLocationResponse.class, headers).body();
        assertNotNull(response);
        assertThat(response.getParentUri()).isEqualTo("gs://dev-datalager-store");
        assertThat(response.getVersion()).isEqualTo("1");
        assertThat(response.getAccessToken()).isEqualTo("dev-read.json-read-token");
        assertThat(response.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void thatDeleteLocationWorks() {

        // Test with two versions.

        // /raw/skatt/dataset : version 1 -> RAW
        // /data/datastore/not-so-sensitive-rawdata

        // /raw/skatt/dataset : version 2 -> SENSITIVE
        // /data/datastore/sensitive-rawdata

        DeleteLocationResponse responseVersion1 = testClient
                .postAsJson("/rpc/DataAccessService/deleteLocation", DeleteLocationRequest.newBuilder()
                                .setPath("/raw/skatt/datasetDeleteTest")
                                .setSnapshot(1)
                                .build(),
                        DeleteLocationResponse.class, headers).body();
        assertThat(responseVersion1.getParentUri()).isEqualTo("file:///data/datastore/not-so-sensitive-rawdata");
        assertThat(responseVersion1.getAccessToken()).isEqualTo("null-write-token");
        assertThat(responseVersion1.getExpirationTime()).isGreaterThan(System.currentTimeMillis());

        DeleteLocationResponse response = testClient
                .postAsJson("/rpc/DataAccessService/deleteLocation", DeleteLocationRequest.newBuilder()
                                .setPath("/raw/skatt/datasetDeleteTest")
                                .setSnapshot(10)
                                .build(),
                        DeleteLocationResponse.class, headers).body();
        assertNotNull(response);
        assertThat(response.getParentUri()).isEqualTo("file:///data/datastore/sensitive-rawdata");
        assertThat(response.getAccessToken()).isEqualTo("null-write-token");
        assertThat(response.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void thatWriteLocationThenGetAccessTokenWorks() {
        WriteLocationResponse writeLocationResponse = testClient.postAsJson("/rpc/DataAccessService/writeLocation", WriteLocationRequest.newBuilder()
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
        assertThat(writeLocationResponse.getAccessToken()).isEqualTo("dev-datalager-store-write-token");
        assertThat(writeLocationResponse.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
        DatasetMeta signedDatasetMeta = ProtobufJsonUtils.toPojo(writeLocationResponse.getValidMetadataJson().toStringUtf8(), DatasetMeta.class);
        assertThat(signedDatasetMeta.getCreatedBy()).isEqualTo("user");
    }

    @Test
    public void thatInvalidUserFails() {
        String[] headers = new String[]{"Authorization", "Bearer " + JWT.create().withClaim("preferred_username", "johndoe")
                .sign(Algorithm.HMAC256("secret"))};

        ReadLocationResponse readResponse = testClient.postAsJson(
                "/rpc/DataAccessService/readLocation",
                ReadLocationRequest.newBuilder()
                        .setPath("/path/to/dataset")
                        .build(),
                ReadLocationResponse.class, headers).body();
        assertNotNull(readResponse);
        assertThat(readResponse.getAccessAllowed()).isFalse();

        WriteLocationResponse writeResponse = testClient.postAsJson(
                "/rpc/DataAccessService/writeLocation",
                WriteLocationRequest.newBuilder()
                        .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                                .setId(DatasetId.newBuilder()
                                        .setPath("/junit/write-loc-and-access-test")
                                        .setVersion(String.valueOf(System.currentTimeMillis()))
                                        .build())
                                .setType(Type.BOUNDED)
                                .setValuation(Valuation.SENSITIVE)
                                .setState(DatasetState.RAW)
                                .build()))
                        .build(),
                WriteLocationResponse.class, headers).body();
        assertNotNull(writeResponse);
        assertThat(writeResponse.getAccessAllowed()).isFalse();
    }
}
