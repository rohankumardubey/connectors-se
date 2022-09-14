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
package org.talend.components.adlsgen2.put;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.I18n;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.standalone.DriverRunner;
import org.talend.sdk.component.api.standalone.RunAtDriver;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "AdlsGen2-put")
@DriverRunner(name = "AdlsGen2Put")
@Documentation("Upload files to Azure Data Lake Storage Gen2")
public class AdlsGen2Put implements Serializable {

    @Service
    private final AdlsGen2Service service;

    private I18n i18n;

    private AdlsGen2PutConfiguration configuration;

    @Connection
    private AdlsGen2Connection injectedConnection;

    public AdlsGen2Put(@Option("configuration") final AdlsGen2PutConfiguration configuration,
            final AdlsGen2Service service, final I18n i18n) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
    }

    @RunAtDriver
    public void upload() {
        if (injectedConnection != null) {
            configuration.setConnection(injectedConnection);
        }
        DataLakeServiceClient serviceClient = service.getDataLakeConnectionClient(configuration.getConnection());
        DataLakeFileSystemClient fileSystemClient = serviceClient.getFileSystemClient(configuration.getFilesystem());

        Map<String, String> fileMap;
        if (configuration.isUseFileList()) {
            List<Map<String, String>> list = new ArrayList<>();
            List<AdlsGen2PutConfiguration.FileMask> fileMasks = configuration.getFiles();
            // process files list
            if (fileMasks != null && fileMasks.size() > 0) {
                for (int idx = 0; idx < fileMasks.size(); idx++) {
                    AdlsGen2PutConfiguration.FileMask fileMask = fileMasks.get(idx);
                    Map<String, String> map = new HashMap<>();
                    map.put(fileMask.getFileMask(), fileMask.getNewName());
                    list.add(map);
                }
            }
            fileMap = genFileFilterList(list, configuration.getLocalFolder(), configuration.getBlobPath(),
                    configuration.isAllowEscapePlusSymbol());
        } else {
            fileMap = genAzureObjectList(new File(configuration.getLocalFolder()), configuration.getBlobPath());
        }
        for (Map.Entry<String, String> entry : fileMap.entrySet()) {
            try {
                // path create
                DataLakeFileClient fileClient = fileSystemClient.getFileClient(entry.getValue());
                File source = new File(entry.getKey());
                if (source.length() > 0) {
                    // TODO TDI-48359 we cannot use Overwrite and Timeout together here
                    fileClient.uploadFromFile(entry.getKey(), configuration.isOverwrite());
                } else {
                    service.pathCreate(serviceClient, configuration.getFilesystem(), entry.getValue(),
                            configuration.isOverwrite(), configuration.getConnection().getTimeout());
                }
            } catch (RuntimeException e) {
                if (configuration.isDieOnError()) {
                    throw new ComponentException(e.getMessage());
                } else {
                    log.error(e.getMessage());
                }
            }
        }
    }

    /**
     *
     */
    public Map<String, String> genAzureObjectList(File file, String keyParent) {
        Map<String, String> map = new HashMap<String, String>();
        if (file.isDirectory()) {
            if (!(keyParent == null || "".equals(keyParent)
                    || keyParent.trim().lastIndexOf("/") == keyParent.trim().length() - 1)) {
                keyParent = new StringBuilder(keyParent).append("/").toString();
            }
            for (File f : file.listFiles()) {
                if (f.isDirectory()) {
                    map.putAll(genAzureObjectList(f,
                            new StringBuilder(String.valueOf(keyParent)).append(f.getName()).append("/").toString()));
                } else {
                    map.put(f.getAbsolutePath(),
                            new StringBuilder(String.valueOf(keyParent)).append(f.getName()).toString());
                }
            }
        } else {
            if (configuration.isDieOnError()) {
                throw new ComponentException(i18n.invalidDirectory());
            } else {
                log.error(i18n.invalidDirectory());
            }
        }
        return map;
    }

    /**
     * @param allowEscapePlusSymbol to add ability to protect '+' symbol in the filemask
     */
    public Map<String, String> genFileFilterList(List<Map<String, String>> list, String localdir, String remotedir,
            boolean allowEscapePlusSymbol) {
        if (remotedir != null) {
            if (!("".equals(remotedir) || remotedir.trim().lastIndexOf("/") == remotedir.trim().length() - 1)) {
                remotedir = new StringBuilder(String.valueOf(remotedir)).append("/").toString();
            }
        }
        Map<String, String> fileMap = new HashMap<>();
        for (Map<String, String> map : list) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                String tempdir = localdir;
                String dir = null;
                // corner case, and workaround param.
                String mask = allowEscapePlusSymbol //
                        ? key.replaceAll("\\\\([^+]|$)", "/$1") // TDI-46283
                        : key.replaceAll("\\\\", "/"); //
                int i = mask.lastIndexOf(47);
                if (i != -1) {
                    dir = mask.substring(0, i);
                    mask = mask.substring(i + 1);
                }
                if (dir != null && !"".equals(dir)) {
                    tempdir = new StringBuilder(String.valueOf(tempdir)).append("/").append(dir).toString();
                }
                mask = mask.replaceAll("\\.", "\\\\.").replaceAll("\\*", ".*");
                String finalMask = mask;
                File[] listings = null;
                File file = new File(tempdir);
                if (file.isDirectory()) {
                    listings = file.listFiles(new LocalFileFilter(finalMask));
                }
                if (listings != null && listings.length > 0) {
                    String localFilePath = "";
                    String newObjectKey = "";
                    for (File listing : listings) {
                        if (listing.getName().matches(mask)) {
                            localFilePath = listing.getAbsolutePath();
                            if (value == null || value.length() <= 0) {
                                newObjectKey = new StringBuilder(String.valueOf(remotedir)).append(listing.getName())
                                        .toString();
                            } else {
                                newObjectKey = new StringBuilder(String.valueOf(remotedir)).append(value).toString();
                            }
                            fileMap.put(localFilePath, newObjectKey);
                        }
                    }
                } else {
                    if (configuration.isDieOnError()) {
                        throw new ComponentException(i18n.errorFileNotExist(key));
                    } else {
                        log.error(i18n.errorFileNotExist(key));
                    }
                }
            }
        }
        return fileMap;
    }

    class LocalFileFilter implements FileFilter {

        private final String mask;

        LocalFileFilter(String str) {
            this.mask = str;
        }

        @Override
        public boolean accept(File pathname) {
            if (pathname == null || !pathname.isFile()) {
                return false;
            }
            return Pattern.compile(this.mask).matcher(pathname.getName()).find();
        }
    }

}
