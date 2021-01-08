package no.ssb.dapla.data.access.service;

import io.helidon.common.reactive.Single;
import io.helidon.metrics.RegistryFactory;
import io.helidon.webclient.WebClient;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class UserAccessWebClient implements UserAccessClient {

    private final WebClient webClient;
    private final Counter userAccessWebClientGetAccessCompleteCount;
    private final Counter userAccessWebClientGetAccessErrorCount;
    private final Counter userAccessWebClientGetAccessCancelCount;

    public UserAccessWebClient(WebClient webClient) {
        this.webClient = webClient;
        RegistryFactory metricsRegistry = RegistryFactory.getInstance();
        MetricRegistry appRegistry = metricsRegistry.getRegistry(MetricRegistry.Type.APPLICATION);
        this.userAccessWebClientGetAccessCompleteCount = appRegistry.counter("userAccessWebClientGetAccessCompleteCount");
        this.userAccessWebClientGetAccessErrorCount = appRegistry.counter("userAccessWebClientGetAccessErrorCount");
        this.userAccessWebClientGetAccessCancelCount = appRegistry.counter("userAccessWebClientGetAccessCancelCount");
    }

    public UserAccessWebClient(URI baseUri) {
        this(WebClient.builder()
                .baseUri(baseUri)
                .build());
    }

    public UserAccessWebClient(String host, int port) {
        this(WebClient.builder()
                .baseUri(toURI(host, port))
                .build());
    }

    private static URI toURI(String host, int port) {
        try {
            return new URI("http", null, host, port, null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public Single<Boolean> hasAccess(String userId, String privilege, String path, String valuation, String state, String jwtToken) {
        return webClient.get()
                .path("/access/" + URLEncoder.encode(userId, StandardCharsets.UTF_8))
                .queryParam("privilege", privilege)
                .queryParam("path", path)
                .queryParam("valuation", valuation)
                .queryParam("state", state)
                .skipUriEncoding()
                .headers(headers -> {
                    headers.put("Authorization", "Bearer " + jwtToken);
                    return headers;
                })
                .request()
                .onComplete(userAccessWebClientGetAccessCompleteCount::inc)
                .onCancel(userAccessWebClientGetAccessCancelCount::inc)
                .onError(throwable -> userAccessWebClientGetAccessErrorCount.inc())
                .map(wcr -> wcr.status().code() == 200);
    }
}
