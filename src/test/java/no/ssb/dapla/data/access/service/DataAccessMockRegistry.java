package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.testing.helidon.MockRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Optional.ofNullable;

public class DataAccessMockRegistry extends MockRegistry {

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

    public DataAccessMockRegistry() {
        add((CatalogClient) (request, jwtToken) -> Single.just(ofNullable(CATALOG.get(request.getPath()))
                .map(dataset -> GetDatasetResponse.newBuilder().setDataset(dataset).build())
                .orElseGet(() -> GetDatasetResponse.newBuilder().build())));
        add((UserAccessClient) (userId, privilege, path, valuation, state, jwtToken) -> Single.just(ACCESS.contains(userId)));
    }
}
