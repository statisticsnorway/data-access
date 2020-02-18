package no.ssb.dapla.data.access.service;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.testing.helidon.GrpcMockRegistry;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(IntegrationTestExtension.class)
@GrpcMockRegistryConfig(DataAccessServiceGrpcTest.DataAccessGrpcMockRegistry.class)
public class DataAccessServiceGrpcTest {

    @Inject
    Channel channel;

    @Test
    public void thatGetAccessTokenWorks() {
        DataAccessServiceGrpc.DataAccessServiceBlockingStub client = DataAccessServiceGrpc.newBlockingStub(channel);
        AccessTokenResponse response = client.getAccessToken(AccessTokenRequest.newBuilder()
                .setPath("/path/to/dataset")
                .setPrivilege(AccessTokenRequest.Privilege.READ)
                .setUserId("user")
                .build());
        assertNotNull(response);
        assertThat(response.getAccessToken()).isEqualTo("localstack-token");
        assertThat(response.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    private static final Map<String, Dataset> CATALOG = new HashMap<>();

    static {

        CATALOG.put(
                "/path/to/dataset",
                Dataset.newBuilder()
                        .setState(Dataset.DatasetState.OUTPUT)
                        .setValuation(Dataset.Valuation.OPEN)
                        .setParentUri("gs://root")
                        .setId(
                                DatasetId.newBuilder()
                                        .setPath("/path/to/dataset")
                                        .setTimestamp(0)
                        )
                        .build()
        );
    }

    private static final Set<String> ACCESS = Set.of("user");

    public static class DataAccessGrpcMockRegistry extends GrpcMockRegistry {
        public DataAccessGrpcMockRegistry() {
            add(new CatalogServiceGrpc.CatalogServiceImplBase() {
                @Override
                public void get(GetDatasetRequest request, StreamObserver<GetDatasetResponse> responseObserver) {

                    GetDatasetResponse.Builder responseBuilder = GetDatasetResponse.newBuilder();

                    Dataset dataset = CATALOG.get(request.getPath());
                    if (dataset != null) {
                        responseBuilder.setDataset(dataset);
                    }
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }
            });

            add(new AuthServiceGrpc.AuthServiceImplBase() {
                @Override
                public void hasAccess(AccessCheckRequest request, StreamObserver<AccessCheckResponse> responseObserver) {
                    AccessCheckResponse.Builder responseBuilder = AccessCheckResponse.newBuilder();

                    if (ACCESS.contains(request.getUserId())) {
                        responseBuilder.setAllowed(true);
                    }

                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }
            });
        }
    }
}
