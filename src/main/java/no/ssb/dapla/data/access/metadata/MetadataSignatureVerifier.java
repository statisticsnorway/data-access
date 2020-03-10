package no.ssb.dapla.data.access.metadata;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;

public class MetadataSignatureVerifier {

    final Signature signature;

    public MetadataSignatureVerifier(String keystoreFormat, String keystorePath, String keyAlias, char[] password, String algorithm) {
        try {
            KeyStore keyStore = KeyStore.getInstance(keystoreFormat);
            keyStore.load(new FileInputStream(keystorePath), password);
            Certificate certificate = keyStore.getCertificate(keyAlias);
            PublicKey publicKey = certificate.getPublicKey();

            signature = Signature.getInstance(algorithm);
            signature.initVerify(publicKey);
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public boolean verify(byte[] data, byte[] receivedSign) {
        try {
            signature.update(data);
            return signature.verify(receivedSign);
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        }
    }

}
