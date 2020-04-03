package no.ssb.dapla.data.access.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
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
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(IntegrationTestExtension.class)
@GrpcMockRegistryConfig(DataAccessGrpcMockRegistry.class)
public class DataAccessServiceGrpcTest {

    @Inject
    Channel channel;

    @Test
    public void thatReadLocationWorks() {
        DataAccessServiceGrpc.DataAccessServiceBlockingStub client = DataAccessServiceGrpc.newBlockingStub(channel)
                .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(JWT.create()
                        .withClaim("preferred_username", "user")
                        .sign(Algorithm.HMAC256("secret"))));
        ReadLocationResponse response = client.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/path/to/dataset")
                .setSnapshot(2)
                .build());
        assertNotNull(response);
        assertThat(response.getParentUri()).isEqualTo("gs://dev-datalager-store");
        assertThat(response.getVersion()).isEqualTo("1");
    }

    @Test
    public void thatReadAccessTokenWorks() {
        DataAccessServiceGrpc.DataAccessServiceBlockingStub client = DataAccessServiceGrpc.newBlockingStub(channel)
                .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(JWT.create()
                        .withClaim("preferred_username", "user")
                        .sign(Algorithm.HMAC256("secret"))));
        ReadAccessTokenResponse response = client.readAccessToken(ReadAccessTokenRequest.newBuilder()
                .setPath("/path/to/dataset")
                .build());
        assertNotNull(response);
        assertThat(response.getParentUri()).isEqualTo("gs://dev-datalager-store");
        // LocalstackDataAccessService doesn't actually generate an access token
        // it just appends 'read-token' to the resolved key file
        assertThat(response.getAccessToken()).isEqualTo("dev-read.json-read-token");
        assertThat(response.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void thatWriteLocationThenGetAccessTokenWorks() {
        DataAccessServiceGrpc.DataAccessServiceBlockingStub client = DataAccessServiceGrpc.newBlockingStub(channel)
                .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(JWT.create()
                        .withClaim("preferred_username", "user")
                        .sign(Algorithm.HMAC256("secret"))));

        WriteLocationResponse writeLocationResponse = client.writeLocation(WriteLocationRequest.newBuilder()
                .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                        .setId(DatasetId.newBuilder()
                                .setPath("/junit/write-loc-and-access-test")
                                .setVersion(String.valueOf(System.currentTimeMillis()))
                                .build())
                        .setType(Type.BOUNDED)
                        .setValuation(Valuation.SENSITIVE)
                        .setState(DatasetState.RAW)
                        .build()))
                .build());

        assertNotNull(writeLocationResponse);
        assertThat(writeLocationResponse.getAccessAllowed()).isTrue();
        DatasetMeta signedDatasetMeta = ProtobufJsonUtils.toPojo(writeLocationResponse.getValidMetadataJson().toStringUtf8(), DatasetMeta.class);
        // TODO ssertThat(signedDatasetMeta.getParentUri()).isEqualTo("gs://dev-datalager-store/datastore");
        assertThat(signedDatasetMeta.getCreatedBy()).isEqualTo("user");

        WriteAccessTokenResponse writeAccessTokenResponse = client.writeAccessToken(WriteAccessTokenRequest.newBuilder()
                .setMetadataJson(writeLocationResponse.getValidMetadataJson())
                .setMetadataSignature(writeLocationResponse.getMetadataSignature())
                .build());

        assertThat(writeAccessTokenResponse.getAccessToken()).isEqualTo("dev-datalager-store-write-token");
        assertThat(writeAccessTokenResponse.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void thatInvalidUserFails() {
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> {
                    DataAccessServiceGrpc.DataAccessServiceBlockingStub client = DataAccessServiceGrpc.newBlockingStub(channel)
                            .withCallCredentials(new GrpcAuthorizationBearerCallCredentials(JWT.create()
                                    .withClaim("preferred_username", "johndoe")
                                    .sign(Algorithm.HMAC256("secret"))));
                    client.readAccessToken(ReadAccessTokenRequest.newBuilder()
                            .setPath("/path/to/dataset")
                            .build());
                },
                "Expect access denied exception");
        assertEquals(exception.getStatus(), Status.PERMISSION_DENIED);
    }
}
