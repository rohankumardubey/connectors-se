/*
 * Copyright (C) 2006-2022 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.adlsgen2.runtime.get;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.get.AdlsGen2Get;
import org.talend.components.adlsgen2.get.AdlsGen2GetConfiguration;
import org.talend.components.adlsgen2.put.AdlsGen2Put;
import org.talend.components.adlsgen2.put.AdlsGen2PutConfiguration;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.adlsgen2")
public class AdlsGen2GetIT extends AdlsGen2IntegrationTestBase {

    AdlsGen2GetConfiguration adlsGen2GetConfiguration;

    @BeforeEach
    void prepare() {
        adlsGen2GetConfiguration = new AdlsGen2GetConfiguration();
        adlsGen2GetConfiguration.setConnection(connection);
        adlsGen2GetConfiguration.setFilesystem(storageFs);
        adlsGen2GetConfiguration.setBlobPath("test-gen2-get");
        adlsGen2GetConfiguration.setDieOnError(true);

        // Upload test files
        AdlsGen2PutConfiguration adlsGen2PutConfiguration = new AdlsGen2PutConfiguration();
        adlsGen2PutConfiguration.setConnection(connection);
        adlsGen2PutConfiguration.setFilesystem(storageFs);
        adlsGen2PutConfiguration.setBlobPath("test-gen2-get");
        adlsGen2PutConfiguration
                .setLocalFolder(getClass().getClassLoader().getResource("azurestorage-get-delete").getPath());

        AdlsGen2Put put = new AdlsGen2Put(adlsGen2PutConfiguration, service, i18n);
        put.upload();
        Assert.assertTrue(blobExits("test-gen2-get/csv/test.csv"));
        Assert.assertTrue(blobExits("test-gen2-get/orc/test.orc"));
        Assert.assertTrue(blobExits("test-gen2-get/parquet/test.parquet"));
        Assert.assertTrue(blobExits("test-gen2-get/summary.txt"));
    }

    @Test
    void testSimpleGet() throws IOException {

        Path localFolderPath = null;
        try {
            localFolderPath = Files.createTempDirectory("test-gen2-get");
            adlsGen2GetConfiguration.setLocalFolder(localFolderPath.toString());

            AdlsGen2Get adlsGen2Get = new AdlsGen2Get(adlsGen2GetConfiguration, service);
            adlsGen2Get.download();

            // check the expected blob path exits
            Assert.assertFalse(new File(localFolderPath + "/test.csv").exists());
            Assert.assertTrue(new File(localFolderPath + "/summary.txt").exists());
        } finally {
            deleteTemp(localFolderPath);
        }
    }

    @Test
    void testSubdirectory() throws IOException {

        Path localFolderPath = null;
        try {
            localFolderPath = Files.createTempDirectory("test-gen2-get");
            adlsGen2GetConfiguration.setLocalFolder(localFolderPath.toString());
            adlsGen2GetConfiguration.setKeepRemoteDirStructure(true);
            adlsGen2GetConfiguration.setIncludeSubDirectory(true);

            AdlsGen2Get adlsGen2Get = new AdlsGen2Get(adlsGen2GetConfiguration, service);
            adlsGen2Get.download();

            // check the expected blob path exits
            File csvFile = new File(localFolderPath + "/test-gen2-get/csv/test.csv");
            Assert.assertTrue(csvFile.exists());
            Assert.assertEquals(255, csvFile.length());

            File orcFile = new File(localFolderPath + "/test-gen2-get/orc/test.orc");
            Assert.assertTrue(orcFile.exists());
            Assert.assertEquals(1473, orcFile.length());

            File parquetFile = new File(localFolderPath + "/test-gen2-get/parquet/test.parquet");
            Assert.assertTrue(parquetFile.exists());
            Assert.assertEquals(1915, parquetFile.length());

            File textFile = new File(localFolderPath + "/test-gen2-get/summary.txt");
            Assert.assertTrue(textFile.exists());
            List<String> lines = Files.readAllLines(Paths.get(textFile.getAbsolutePath()));
            Assert.assertEquals(1, lines.size());
            Assert.assertEquals("this is for adlsgen2 get test!", lines.get(0));
        } finally {
            deleteTemp(localFolderPath);
        }
    }

    private void deleteTemp(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        if (!Files.isDirectory(path)) {
            Files.delete(path);
        } else {
            Iterator<Path> iterator = Files.list(path).iterator();
            while (iterator.hasNext()) {
                deleteTemp(iterator.next());
            }
            File emptyDir = new File(path.toString());
            emptyDir.delete();
        }
    }
}
