package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.testing.helidon.MockRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.util.Optional.ofNullable;
import static no.ssb.dapla.catalog.protobuf.Dataset.Valuation;
import static no.ssb.dapla.catalog.protobuf.Dataset.newBuilder;

public class DataAccessMockRegistry extends MockRegistry {

    private static final Map<String, SortedMap<Long, Dataset>> CATALOG = new HashMap<>();

    private static final Set<String> ACCESS = Set.of("user");

    static {
        addDataset(newBuilder()
                .setState(Dataset.DatasetState.OUTPUT)
                .setValuation(Valuation.OPEN)
                .setParentUri("gs://dev-datalager-store")
                .setId(
                        DatasetId.newBuilder()
                                .setPath("/path/to/dataset")
                                .setTimestamp(1)
                )
                .build());
    }

    public DataAccessMockRegistry() {
        add((CatalogClient) DataAccessMockRegistry::getDataset);
        add((UserAccessClient) (userId, privilege, path, valuation, state, jwtToken) -> Single.just(ACCESS.contains(userId)));
    }

    static Single<GetDatasetResponse> getDataset(GetDatasetRequest request, String jwtToken) {
        return Single.just(ofNullable(CATALOG.get(request.getPath()))
                .map(versions -> versions.headMap(request.getTimestamp() + 1))
                .flatMap(subVersions -> ofNullable(subVersions.get(subVersions.lastKey())))
                .map(dataset -> GetDatasetResponse.newBuilder().setDataset(dataset).build())
                .orElse(GetDatasetResponse.newBuilder().build()));
    }

    public static void addDataset(String path, Long version, Dataset.DatasetState state, Valuation valuation, String parentURI) {
        addDataset(newBuilder()
                .setState(state)
                .setValuation(valuation)
                .setParentUri(parentURI)
                .setId(
                        DatasetId.newBuilder()
                                .setPath(path)
                                .setTimestamp(version)
                )
                .build());
    }

    public static void addDataset(Dataset dataset) {
        String path = dataset.getId().getPath();
        Long timestamp = dataset.getId().getTimestamp();
        CATALOG.computeIfAbsent(path, s -> new TreeMap<>()).put(timestamp, dataset);
    }
}
