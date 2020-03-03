package no.ssb.dapla.data.access.service;

public class AccessToken {

    private final String accessToken;
    private final long expirationTime;
    private final String parentUri;

    public AccessToken(String accessToken, long expirationTime, String parentUri) {
        this.accessToken = accessToken;
        this.expirationTime = expirationTime;
        this.parentUri = parentUri;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public String getParentUri() {
        return parentUri;
    }
}
