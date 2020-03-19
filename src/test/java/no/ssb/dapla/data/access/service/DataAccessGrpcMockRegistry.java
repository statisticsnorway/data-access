package no.ssb.dapla.data.access.service;

import io.grpc.stub.StreamObserver;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.testing.helidon.GrpcMockRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataAccessGrpcMockRegistry extends GrpcMockRegistry {

    private static final Map<String, Dataset> CATALOG = new HashMap<>();

    private static final Set<String> ACCESS = Set.of("user");

    static {

        CATALOG.put(
                "/path/to/dataset",
                Dataset.newBuilder()
                        .setState(Dataset.DatasetState.OUTPUT)
                        .setValuation(Dataset.Valuation.OPEN)
                        .setParentUri("gs://dev-datalager-store")
                        .setId(
                                DatasetId.newBuilder()
                                        .setPath("/path/to/dataset")
                                        .setTimestamp(1)
                        )
                        .build()
        );
    }

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
