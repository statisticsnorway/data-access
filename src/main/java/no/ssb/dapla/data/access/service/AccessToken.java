package no.ssb.dapla.data.access.service;

public class AccessToken {

    private final String accessToken;
    private final long expirationTime;

    public AccessToken(String accessToken, long expirationTime) {
        this.accessToken = accessToken;
        this.expirationTime = expirationTime;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpirationTime() {
        return expirationTime;
    }
}
