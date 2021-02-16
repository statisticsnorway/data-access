package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;

public interface CatalogClient {

    Single<GetDatasetResponse> get(GetDatasetRequest request, String jwtToken);

    default Single<GetDatasetResponse> get(String path, Long version, String token) {
        var request = GetDatasetRequest.newBuilder()
                .setPath(path)
                .setTimestamp(version)
                .build();
        return get(request, token);
    }
}
