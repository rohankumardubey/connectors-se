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
package org.talend.components.adlsgen2.get;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.standalone.DriverRunner;
import org.talend.sdk.component.api.standalone.RunAtDriver;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "AdlsGen2-get")
@DriverRunner(name = "AdlsGen2Get")
@Documentation("Fetch files from Azure Data Lake Storage Gen2")
public class AdlsGen2Get implements Serializable {

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    private final AdlsGen2Service service;

    private AdlsGen2GetConfiguration configuration;

    @Connection
    private AdlsGen2Connection injectedConnection;

    public AdlsGen2Get(@Option("configuration") final AdlsGen2GetConfiguration configuration,
            final AdlsGen2Service service) {
        this.configuration = configuration;
        this.service = service;
    }

    @RunAtDriver
    public void download() {
        if (injectedConnection != null) {
            configuration.setConnection(injectedConnection);
        }
        String folder = configuration.getLocalFolder();
        try {
            List<BlobInformations> blobs =
                    service.getBlobs(configuration.getConnection(), configuration.getFilesystem(),
                            configuration.getBlobPath(), null, configuration.isIncludeSubDirectory());
            for (BlobInformations blobInfo : blobs) {
                try (InputStream currentItemInputStream =
                        service.getBlobInputstream(configuration.getConnection(), configuration.getFilesystem(),
                                blobInfo)) {
                    String resultFileName = blobInfo.getBlobPath();
                    if (!configuration.isKeepRemoteDirStructure()) {
                        if (resultFileName.contains("/")) {
                            resultFileName = resultFileName.substring(resultFileName.lastIndexOf("/"));
                        }
                    }
                    File file = new File(folder + "/" + resultFileName);
                    if (!file.getParentFile().exists()) {
                        file.getParentFile().mkdirs();
                    }
                    try (java.io.OutputStream fos = new java.io.FileOutputStream(file)) {
                        IOUtils.copy(currentItemInputStream, fos);
                    }
                }
            }
        } catch (IOException e) {
            if (configuration.isDieOnError()) {
                throw new AdlsGen2RuntimeException(e.getMessage());
            } else {
                log.error(e.getMessage());
            }
        }
    }

}
