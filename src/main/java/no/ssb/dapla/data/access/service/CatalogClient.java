package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;

public interface CatalogClient {

    Single<GetDatasetResponse> get(GetDatasetRequest request, String jwtToken);

}
