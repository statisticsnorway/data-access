package no.ssb.dapla.data.access.metadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class MetadataSigner {

    final Signature signature;

    public MetadataSigner(String keystoreFormat, String keystorePath, String keyAlias, char[] password, String algorithm) {
        try {
            KeyStore keyStore = KeyStore.getInstance(keystoreFormat);
            keyStore.load(new FileInputStream(keystorePath), password);
            PrivateKey privateKey = (PrivateKey) keyStore.getKey(keyAlias, password);

            signature = Signature.getInstance(algorithm);
            signature.initSign(privateKey);
        } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | InvalidKeyException | UnrecoverableKeyException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] sign(byte[] data) {
        try {
            signature.update(data);
            return signature.sign();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        }
    }
}
