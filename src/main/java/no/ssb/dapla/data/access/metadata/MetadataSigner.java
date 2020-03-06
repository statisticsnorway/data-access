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

    public MetadataSigner() {
        try {
            char[] password = new char[]{'c', 'h', 'a', 'n', 'g', 'e', 'i', 't'};
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(new FileInputStream("secret/metadata-signer_keystore.p12"), password);
            PrivateKey privateKey = (PrivateKey) keyStore.getKey("dataAccessKeyPair", password);

            signature = Signature.getInstance("SHA256withRSA");
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
