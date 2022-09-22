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
package org.talend.components.adlsgen2.delete;

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.standalone.DriverRunner;
import org.talend.sdk.component.api.standalone.RunAtDriver;

import java.io.Serializable;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "AdlsGen2-delete")
@DriverRunner(name = "AdlsGen2Delete")
@Documentation("Delete blobs from a Azure Data Lake Storage Gen2 container")
public class AdlsGen2Delete implements Serializable {

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    private final AdlsGen2Service service;

    private AdlsGen2DeleteConfiguration configuration;

    @Connection
    private AdlsGen2Connection injectedConnection;

    public AdlsGen2Delete(@Option("configuration") final AdlsGen2DeleteConfiguration configuration,
            final AdlsGen2Service service) {
        this.configuration = configuration;
        this.service = service;
    }

    @RunAtDriver
    public void delete() {
        if (injectedConnection != null) {
            configuration.setConnection(injectedConnection);
        }
        try {

            DataLakeServiceClient serviceClient = service.getDataLakeConnectionClient(configuration.getConnection());
            DataLakeFileSystemClient fileSystemClient =
                    serviceClient.getFileSystemClient(configuration.getFilesystem());
            DataLakeDirectoryClient directoryClient =
                    fileSystemClient.getDirectoryClient(configuration.getBlobPath());
            directoryClient.deleteWithResponse(configuration.isRecursive(), null,
                    Duration.of(configuration.getConnection().getTimeout().longValue(), SECONDS), Context.NONE);
        } catch (RuntimeException e) {
            if (configuration.isDieOnError()) {
                throw new AdlsGen2RuntimeException(e.getMessage());
            } else {
                log.error(e.getMessage());
            }
        }
    }

}
