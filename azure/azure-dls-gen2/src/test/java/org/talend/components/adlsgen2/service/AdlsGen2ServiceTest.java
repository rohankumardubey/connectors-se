package org.talend.components.adlsgen2.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdlsGen2ServiceTest {

    @Test
    void extractFileNameWithSpecialCharactersTest() {
        String linuxSupportedFileName = "someDir/file*\"<a!b;c>.txt";
        AdlsGen2Service service = new AdlsGen2Service();
        String fileName = service.extractFileName(linuxSupportedFileName);

        assertEquals(linuxSupportedFileName.substring(linuxSupportedFileName.lastIndexOf("/")), fileName);
    }

    @Test
    void extractDirNameWithSpecialCharactersTest() {
        String linuxSupportedFileName = "someDir\"*Dir;Name<>!/file*\"<abc>.txt";
        AdlsGen2Service service = new AdlsGen2Service();
        String dirName = service.extractFolderPath(linuxSupportedFileName);

        assertEquals(linuxSupportedFileName.substring(0, linuxSupportedFileName.lastIndexOf("/")), dirName);
    }
}