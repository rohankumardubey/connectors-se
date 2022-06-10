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
package org.talend.components.adlsgen2.runtime.delete;

import java.nio.file.Path;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.delete.AdlsGen2Delete;
import org.talend.components.adlsgen2.delete.AdlsGen2DeleteConfiguration;
import org.talend.components.adlsgen2.put.AdlsGen2Put;
import org.talend.components.adlsgen2.put.AdlsGen2PutConfiguration;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.adlsgen2")
public class AdlsGen2DeleteIT extends AdlsGen2IntegrationTestBase {

    AdlsGen2DeleteConfiguration gen2DeleteConfig;

    @BeforeEach
    void prepare() {
        gen2DeleteConfig = new AdlsGen2DeleteConfiguration();
        gen2DeleteConfig.setConnection(connection);
        gen2DeleteConfig.setFilesystem(storageFs);
        gen2DeleteConfig.setBlobPath("test-gen2-delete");
        gen2DeleteConfig.setDieOnError(true);

        // Upload test files
        AdlsGen2PutConfiguration adlsGen2PutConfiguration = new AdlsGen2PutConfiguration();
        adlsGen2PutConfiguration.setConnection(connection);
        adlsGen2PutConfiguration.setFilesystem(storageFs);
        adlsGen2PutConfiguration.setBlobPath("test-gen2-delete");
        adlsGen2PutConfiguration
                .setLocalFolder(getClass().getClassLoader().getResource("azurestorage-get-delete").getPath());

        AdlsGen2Put put = new AdlsGen2Put(adlsGen2PutConfiguration, service, i18n);
        put.upload();
        Assert.assertTrue(blobExits("test-gen2-delete/csv/test.csv"));
        Assert.assertTrue(blobExits("test-gen2-delete/orc/test.orc"));
        Assert.assertTrue(blobExits("test-gen2-delete/parquet/test.parquet"));
        Assert.assertTrue(blobExits("test-gen2-delete/summary.txt"));
    }

    @Test
    void testSimpleDelete() {
        AdlsGen2Delete adlsGen2Delete = new AdlsGen2Delete(gen2DeleteConfig, service);
        try {
            adlsGen2Delete.delete();
            Assert.fail("Should delete failed!");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("DirectoryNotEmpty"));
        }
        // check the expected blob path exits
        Assert.assertTrue(blobExits("test-gen2-delete/csv/test.csv"));
        Assert.assertTrue(blobExits("test-gen2-delete/orc/test.orc"));
        Assert.assertTrue(blobExits("test-gen2-delete/parquet/test.parquet"));
        Assert.assertTrue(blobExits("test-gen2-delete/summary.txt"));

        gen2DeleteConfig.setRecursive(true);
        adlsGen2Delete.delete();
        Assert.assertFalse(blobExits("test-gen2-delete"));
    }
}
