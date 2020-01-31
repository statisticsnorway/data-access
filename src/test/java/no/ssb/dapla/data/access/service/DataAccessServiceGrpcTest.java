package no.ssb.dapla.data.access.service;

import io.grpc.Channel;
import no.ssb.dapla.data.access.Application;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(IntegrationTestExtension.class)
public class DataAccessServiceGrpcTest {

    @Inject
    Application application;

    @Inject
    Channel channel;

    @Test
    public void thatGetAccessTokenWorks() {
        DataAccessServiceGrpc.DataAccessServiceBlockingStub client = DataAccessServiceGrpc.newBlockingStub(channel);
        AccessTokenResponse response = client.getAccessToken(AccessTokenRequest.newBuilder()
                .setLocation("location")
                .setPrivilege(AccessTokenRequest.Privilege.READ)
                .setUserId("user")
                .build());
        assertNotNull(response);
    }
}
