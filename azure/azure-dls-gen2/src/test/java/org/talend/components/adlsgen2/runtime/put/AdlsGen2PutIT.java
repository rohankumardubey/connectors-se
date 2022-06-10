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
package org.talend.components.adlsgen2.runtime.put;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.put.AdlsGen2Put;
import org.talend.components.adlsgen2.put.AdlsGen2PutConfiguration;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.I18n;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.ArrayList;
import java.util.List;

@WithComponents("org.talend.components.adlsgen2")
public class AdlsGen2PutIT extends AdlsGen2IntegrationTestBase {

    AdlsGen2PutConfiguration adlsGen2PutConfiguration;

    private String localFolderPath;

    @BeforeEach
    void initDataset() {
        adlsGen2PutConfiguration = new AdlsGen2PutConfiguration();
        adlsGen2PutConfiguration.setConnection(connection);
        adlsGen2PutConfiguration.setFilesystem(storageFs);
        localFolderPath = getClass().getClassLoader().getResource("azurestorage-put").getPath();
    }

    @Test
    void testSimplePut() {
        adlsGen2PutConfiguration.setBlobPath("simple-put");
        adlsGen2PutConfiguration.setLocalFolder(localFolderPath + "/sub1");
        AdlsGen2Put put = new AdlsGen2Put(adlsGen2PutConfiguration, service, i18n);
        put.upload();

        // check the expected blob path exits
        Assert.assertTrue(blobExits("simple-put/sub1blob1.txt"));
        Assert.assertTrue(blobExits("simple-put/sub1blob2.txt"));
        Assert.assertTrue(blobExits("simple-put/sub1blob3.txt"));
        Assert.assertTrue(blobExits("simple-put/sub2/sub2blob1.txt"));
        Assert.assertTrue(blobExits("simple-put/sub2/sub2blob2.txt"));
        Assert.assertTrue(blobExits("simple-put/sub2/sub2blob3.txt"));
    }

    @Test
    void testUseFileList() {
        adlsGen2PutConfiguration.setBlobPath("use-file-list");
        adlsGen2PutConfiguration.setLocalFolder(localFolderPath);

        adlsGen2PutConfiguration.setUseFileList(true);
        List<AdlsGen2PutConfiguration.FileMask> fileMaskList = new ArrayList<>();
        AdlsGen2PutConfiguration.FileMask fileMask1 = new AdlsGen2PutConfiguration.FileMask();
        fileMask1.setFileMask("sub3/sub3blob1.txt");
        fileMask1.setNewName("");
        AdlsGen2PutConfiguration.FileMask fileMask2 = new AdlsGen2PutConfiguration.FileMask();
        fileMask2.setFileMask("blob1.txt");
        fileMask2.setNewName("new_blob1.txt");
        AdlsGen2PutConfiguration.FileMask fileMask3 = new AdlsGen2PutConfiguration.FileMask();
        fileMask3.setFileMask("blob+.txt");
        fileMask3.setNewName("blob-new.txt");
        fileMaskList.add(fileMask1);
        fileMaskList.add(fileMask2);
        fileMaskList.add(fileMask3);
        adlsGen2PutConfiguration.setFiles(fileMaskList);
        AdlsGen2Put put = new AdlsGen2Put(adlsGen2PutConfiguration, service, i18n);
        put.upload();

        // check the expected blob path exits
        Assert.assertTrue(blobExits("use-file-list/sub3blob1.txt"));
        Assert.assertTrue(blobExits("use-file-list/new_blob1.txt"));
        Assert.assertTrue(blobExits("use-file-list/blob-new.txt"));
    }

    @Test
    void testEscapePlusSymbol() {
        adlsGen2PutConfiguration.setBlobPath("use-file-list");
        adlsGen2PutConfiguration.setLocalFolder(localFolderPath);

        adlsGen2PutConfiguration.setUseFileList(true);
        List<AdlsGen2PutConfiguration.FileMask> fileMaskList = new ArrayList<>();
        AdlsGen2PutConfiguration.FileMask fileMask1 = new AdlsGen2PutConfiguration.FileMask();
        fileMask1.setFileMask("blob\\+.txt");
        fileMask1.setNewName("");
        fileMaskList.add(fileMask1);
        adlsGen2PutConfiguration.setFiles(fileMaskList);
        adlsGen2PutConfiguration.setAllowEscapePlusSymbol(true);
        adlsGen2PutConfiguration.setDieOnError(true);
        AdlsGen2Put put = new AdlsGen2Put(adlsGen2PutConfiguration, service, i18n);
        put.upload();

        // check the expected blob path exits
        Assert.assertTrue(blobExits("use-file-list/blob+.txt"));
        Assert.assertFalse(blobExits("use-file-list/blob1.txt"));
    }

    @Test
    void testOverwriteChecked() {
        adlsGen2PutConfiguration.setBlobPath("use-file-list");
        adlsGen2PutConfiguration.setLocalFolder(localFolderPath);

        adlsGen2PutConfiguration.setUseFileList(true);
        List<AdlsGen2PutConfiguration.FileMask> fileMaskList = new ArrayList<>();
        AdlsGen2PutConfiguration.FileMask fileMask1 = new AdlsGen2PutConfiguration.FileMask();
        fileMask1.setFileMask("blob1.txt");
        fileMask1.setNewName("");
        fileMaskList.add(fileMask1);
        adlsGen2PutConfiguration.setFiles(fileMaskList);
        adlsGen2PutConfiguration.setAllowEscapePlusSymbol(true);
        adlsGen2PutConfiguration.setDieOnError(true);
        AdlsGen2Put put = new AdlsGen2Put(adlsGen2PutConfiguration, service, i18n);
        put.upload();
        // whether the expected blob path exits
        Assert.assertTrue(blobExits("use-file-list/blob1.txt"));

        // upload again with "Overwrite existing files" unchecked
        try {
            put.upload();
            Assert.fail("Should upload failed!");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("PathAlreadyExists"));
        }

        // checked "Overwrite existing files"
        adlsGen2PutConfiguration.setOverwrite(true);
        put.upload();
    }
}
