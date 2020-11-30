package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import io.helidon.webclient.WebClient;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;

import java.net.URI;
import java.net.URISyntaxException;

public class CatalogWebClient implements CatalogClient {

    private final WebClient webClient;

    public CatalogWebClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public CatalogWebClient(URI baseUri) {
        this.webClient = WebClient.builder()
                .baseUri(baseUri)
                .build();
    }

    public CatalogWebClient(String host, int port) {
        try {
            this.webClient = WebClient.builder()
                    .baseUri(new URI("http", null, host, port, null, null, null))
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Single<GetDatasetResponse> get(GetDatasetRequest request, String jwtToken) {
        return webClient.post()
                .path("/rpc/CatalogService/get")
                .headers(headers -> {
                    headers.put("Authorization", "Bearer: " + jwtToken);
                    return headers;
                })
                .submit(request, GetDatasetResponse.class);
    }
}
