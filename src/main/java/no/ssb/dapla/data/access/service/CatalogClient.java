package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetTableRequest;
import no.ssb.dapla.catalog.protobuf.GetTableResponse;

public interface CatalogClient {

    Single<GetDatasetResponse> get(GetDatasetRequest request, String jwtToken);

    Single<GetTableResponse> get(GetTableRequest request, String jwtToken);

    default Single<GetDatasetResponse> get(String path, Long version, String token) {
        var request = GetDatasetRequest.newBuilder()
                .setPath(path)
                .setTimestamp(version)
                .build();
        return get(request, token);
    }
}
