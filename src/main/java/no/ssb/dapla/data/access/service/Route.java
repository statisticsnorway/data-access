package no.ssb.dapla.data.access.service;

import io.helidon.config.Config;

import java.net.URI;
import java.util.Map;

public class Route {

    private final URI uri;
    private final Map<String, String> auth;

    public Route(Config config) {
        this.uri = config.get("target").get("uri").as(uri ->
                URI.create(uri.get("scheme").asString().get() + "://" +
                        uri.get("host").asString().get() +
                        uri.get("path-prefix").asString().get()
                )).get();
        this.auth = config.get("target").get("auth").detach().asMap().get();
    }

    public URI getUri() {
        return uri;
    }

    public Map<String, String> getAuth() {
        return auth;
    }
}
