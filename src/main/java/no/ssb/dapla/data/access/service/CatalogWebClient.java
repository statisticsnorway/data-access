package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import io.helidon.metrics.RegistryFactory;
import io.helidon.webclient.WebClient;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.catalog.protobuf.GetTableRequest;
import no.ssb.dapla.catalog.protobuf.GetTableResponse;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;

import java.net.URI;
import java.net.URISyntaxException;

public class CatalogWebClient implements CatalogClient {

    private final WebClient webClient;

    private final Counter catalogWebClientGetAccessCompleteCount;
    private final Counter catalogWebClientGetAccessErrorCount;
    private final Counter catalogWebClientGetAccessCancelCount;

    public CatalogWebClient(WebClient webClient) {
        this.webClient = webClient;
        RegistryFactory metricsRegistry = RegistryFactory.getInstance();
        MetricRegistry appRegistry = metricsRegistry.getRegistry(MetricRegistry.Type.APPLICATION);
        this.catalogWebClientGetAccessCompleteCount = appRegistry.counter("catalogWebClientGetAccessCompleteCount");
        this.catalogWebClientGetAccessErrorCount = appRegistry.counter("catalogWebClientGetAccessErrorCount");
        this.catalogWebClientGetAccessCancelCount = appRegistry.counter("catalogWebClientGetAccessCancelCount");
    }

    public CatalogWebClient(URI baseUri) {
        this(WebClient.builder()
                .addMediaSupport(ProtobufJsonSupport.create())
                .baseUri(baseUri)
                .build());
    }

    public CatalogWebClient(String host, int port) {
        this(WebClient.builder()
                .addMediaSupport(ProtobufJsonSupport.create())
                .baseUri(toUri(host, port))
                .build());
    }

    private static URI toUri(String host, int port) {
        try {
            return new URI("http", null, host, port, null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Single<GetDatasetResponse> get(GetDatasetRequest request, String jwtToken) {
        return webClient.post()
                .path("/rpc/CatalogService/get")
                .headers(headers -> {
                    headers.put("Authorization", "Bearer " + jwtToken);
                    return headers;
                })
                .submit(request, GetDatasetResponse.class)
                .onComplete(catalogWebClientGetAccessCompleteCount::inc)
                .onCancel(catalogWebClientGetAccessCancelCount::inc)
                .onError(throwable -> catalogWebClientGetAccessErrorCount.inc());
    }

    @Override
    public Single<GetTableResponse> get(GetTableRequest request, String jwtToken) {
        return webClient.post()
                .path("/catalog2/get")
                .headers(headers -> {
                    headers.put("Authorization", "Bearer " + jwtToken);
                    return headers;
                })
                .submit(request, GetTableResponse.class);
    }
}
