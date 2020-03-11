package no.ssb.dapla.data.access.metadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

public class MetadataSignerTest {

    MetadataSigner metadataSigner = new MetadataSigner(
            "PKCS12",
            "src/test/resources/metadata-signer_keystore.p12",
            "dataAccessKeyPair",
            "changeit".toCharArray(),
            "SHA256withRSA"
    );

    //@Test
    public void thatMetadataSignWorks() throws IOException {
        signDatasets("../localstack/data/datastore",
                "/ske/sirius-person-utkast/2018v19/1583156472183",
                "/skatt/person/rawdata-2019/1582719098762"
        );
    }

    //@Test
    public void thatMetadataVerifyWorks() throws IOException {
        verifyDatasets("../localstack/data/datastore",
                "/ske/sirius-person-utkast/2018v19/1583156472183",
                "/skatt/person/rawdata-2019/1582719098762"
        );
    }

    private void signDatasets(String dataFolder, String... paths) throws IOException {
        for (String path : paths) {
            sign(dataFolder + path + "/.dataset-meta.json",
                    dataFolder + path + "/.dataset-meta.json.sign"
            );
        }
    }

    private void verifyDatasets(String dataFolder, String... paths) throws IOException {
        for (String path : paths) {
            boolean valid = verify(dataFolder + path + "/.dataset-meta.json",
                    dataFolder + path + "/.dataset-meta.json.sign"
            );
            assertThat(valid).isTrue();
        }
    }

    private void sign(String fileToSign, String signatureFileToWrite) throws IOException {
        byte[] data = Files.readAllBytes(Path.of(fileToSign));
        byte[] signature = metadataSigner.sign(data);
        Files.write(Path.of(signatureFileToWrite), signature, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private boolean verify(String fileToVerify, String signatureFile) throws IOException {
        byte[] data = Files.readAllBytes(Path.of(fileToVerify));
        byte[] providedSignature = Files.readAllBytes(Path.of(signatureFile));
        boolean valid = metadataSigner.verify(data, providedSignature);
        return valid;
    }
}
