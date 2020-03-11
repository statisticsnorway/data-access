package no.ssb.dapla.data.access.metadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MetadataSignerTest {

    //@Test
    public void thatMetadataSignerWorks() throws IOException {
        MetadataSigner metadataSigner = new MetadataSigner(
                "PKCS12",
                "src/test/resources/metadata-signer_keystore.p12",
                "dataAccessKeyPair",
                "changeit".toCharArray(),
                "SHA256withRSA"
        );

        signDatasets(metadataSigner, "../localstack/data/datastore",
                "/ske/sirius-person-utkast/2018v19/1583156472183",
                "/skatt/person/rawdata-2019/1582719098762"
        );
    }

    private void signDatasets(MetadataSigner metadataSigner, String dataFolder, String... paths) throws IOException {
        for (String path : paths) {
            sign(metadataSigner, dataFolder + path + "/.dataset-meta.json",
                    dataFolder + path + "/.dataset-meta.json.sign"
            );
        }
    }

    private void sign(MetadataSigner metadataSigner, String fileToSign, String signatureFileToWrite) throws IOException {
        byte[] data = Files.readAllBytes(Path.of(fileToSign));
        byte[] signature = metadataSigner.sign(data);
        Files.write(Path.of(signatureFileToWrite), signature, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
