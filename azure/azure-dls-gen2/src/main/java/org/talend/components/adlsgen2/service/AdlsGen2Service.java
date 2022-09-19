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
package org.talend.components.adlsgen2.service;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Configuration;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.FileSystemItem;
import com.azure.storage.file.datalake.models.ListFileSystemsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.sdk.component.api.service.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
@Service
public class AdlsGen2Service {

    @SuppressWarnings("unchecked")
    public List<String> filesystemList(final AdlsGen2Connection connection) {
        DataLakeServiceClient client = getDataLakeConnectionClient(connection);
        return client
                .listFileSystems(new ListFileSystemsOptions(),
                        Duration.of(connection.getTimeout().longValue(), SECONDS))
                .stream()
                .map(FileSystemItem::getName)
                .collect(Collectors.toList());
    }

    public String extractFolderPath(String blobPath) {
        String folderPath;
        if (blobPath.contains("/")) {
            folderPath = blobPath.substring(0, blobPath.lastIndexOf("/"));
        } else {
            folderPath = "/";
        }

        log.debug("[extractFolderPath] blobPath: {}. Path: {}.", blobPath, folderPath);

        return folderPath;
    }

    public String extractFileName(String blobPath) {
        if (!blobPath.contains("/")) {
            return blobPath;
        }
        String fileName = blobPath.substring(blobPath.lastIndexOf("/"));
        log.debug("[extractFileName] blobPath: {}. Path: {}.", blobPath, fileName);
        return fileName;
    }

    public List<BlobInformations> getBlobs(final AdlsGen2DataSet dataSet) {
        return getBlobs(dataSet.getConnection(), dataSet.getFilesystem(), dataSet.getBlobPath(), dataSet.getFormat(),
                false);
    }

    public boolean blobExists(AdlsGen2DataSet dataSet, String blobName) {
        return getDataLakeConnectionClient(dataSet.getConnection())
                .getFileSystemClient(dataSet.getFilesystem())
                .getDirectoryClient(extractFolderPath(blobName))
                .getFileClient(extractFileName(blobName))
                .existsWithResponse(Duration.of(dataSet.getConnection().getTimeout().longValue(), SECONDS),
                        Context.NONE)
                .getValue();
    }

    @SuppressWarnings("unchecked")
    public InputStream getBlobInputstream(AdlsGen2DataSet dataSet, BlobInformations blob) throws IOException {
        return getBlobInputstream(dataSet.getConnection(), dataSet.getFilesystem(), blob);
    }

    public InputStream getBlobInputstream(AdlsGen2Connection adlsGen2Connection, String fileSystem,
            BlobInformations blob) throws IOException {
        DataLakeFileClient blobFileClient = getDataLakeConnectionClient(adlsGen2Connection)
                .getFileSystemClient(fileSystem)
                .getDirectoryClient(blob.getDirectory())
                .getFileClient(blob.getFileName());
        return blobFileClient.openInputStream().getInputStream();

    }

    @SuppressWarnings("unchecked")
    public boolean pathCreate(AdlsGen2DataSet dataSet) {
        DataLakeServiceClient client = getDataLakeConnectionClient(dataSet.getConnection());
        return pathCreate(client, dataSet.getFilesystem(), dataSet.getBlobPath(), true,
                dataSet.getConnection().getTimeout());
    }

    public boolean pathCreate(DataLakeServiceClient client, String fileSystem, String blobPath, boolean overwrite,
            Integer timeout) {
        DataLakeFileSystemClient fsClient =
                client.getFileSystemClient(fileSystem);
        // TODO is it OK to have current file path in dataset in the folder option?
        DataLakeRequestConditions requestConditions = new DataLakeRequestConditions();
        if (!overwrite) {
            requestConditions.setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD);
        }
        final DataLakeFileClient fileClient = fsClient
                .createFileWithResponse(blobPath, null, null, null, null, requestConditions,
                        Duration.of(timeout, SECONDS), Context.NONE)
                .getValue();
        return fileClient.existsWithResponse(Duration.of(timeout, SECONDS), Context.NONE).getValue();
    }

    @SuppressWarnings("unchecked")
    public void pathUpdate(AdlsGen2DataSet dataSet, byte[] content, long position) {
        DataLakeServiceClient client = getDataLakeConnectionClient(dataSet.getConnection());
        DataLakeFileSystemClient fsClient =
                client.getFileSystemClient(dataSet.getFilesystem());
        DataLakeFileClient fileClient = fsClient.getFileClient(dataSet.getBlobPath());
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content)) {
            fileClient.appendWithResponse(inputStream, position, content.length, null, null,
                    Duration.of(dataSet.getConnection().getTimeout().longValue(), SECONDS), Context.NONE);
        } catch (IOException e) {
            log.warn("[Problem here]", e); // FIXME
        }
    }

    /**
     * To flush, the previously uploaded data must be contiguous, the position parameter must be specified and equal to
     * the length of the file after all data has been written, and there must not be a request entity body included
     * with the request.
     *
     * @param dataSet
     * @param position
     */
    @SuppressWarnings("unchecked")
    public void flushBlob(AdlsGen2DataSet dataSet, long position) {
        DataLakeServiceClient client = getDataLakeConnectionClient(dataSet.getConnection());
        DataLakeFileSystemClient fsClient =
                client.getFileSystemClient(dataSet.getFilesystem());

        DataLakeFileClient fileClient = fsClient.getFileClient(dataSet.getBlobPath());
        DataLakeRequestConditions requestConditions = new DataLakeRequestConditions();
        fileClient
                .flushWithResponse(position, false, false, null, requestConditions,
                        Duration.of(dataSet.getConnection().getTimeout().longValue(), SECONDS), Context.NONE)
                .getValue();

    }

    public DataLakeServiceClient getDataLakeConnectionClient(AdlsGen2Connection connection) {
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        switch (connection.getAuthMethod()) {
        case SharedKey:
            builder = builder.credential(new StorageSharedKeyCredential(connection.getAccountName(),
                    connection.getSharedKey()));
            break;
        case SAS:
            builder = builder.credential(new AzureSasCredential(connection.getSas()));
            break;
        case ActiveDirectory:
            builder = builder.credential(new ClientSecretCredentialBuilder()
                    .tenantId(connection.getTenantId())
                    .clientId(connection.getClientId())
                    .clientSecret(connection.getClientSecret())
                    .build());
        }
        final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder()
                .putProperty(Configuration.PROPERTY_AZURE_REQUEST_CONNECT_TIMEOUT,
                        String.valueOf(connection.getTimeout() * 1000));
        return builder.endpoint(connection.apiUrl())
                .configuration(configurationBuilder.build())
                .buildClient();
    }

    public List<BlobInformations> getBlobs(AdlsGen2Connection adlsGen2Connection, String filesystem,
            String blobPath, FileFormat format, boolean includeSubDirectory) {
        if (format == FileFormat.DELTA) {
            // delta format is a "directory" contains parquet files and subdir with json and crc files, so no need to
            // fetch all child paths.
            // TODO check if we can obtain it with recursive=true in URL
            List<BlobInformations> result = new ArrayList<>();
            BlobInformations info = new BlobInformations();
            info.setBlobPath(blobPath);
            result.add(info);
            return result;
        }

        DataLakeServiceClient client = getDataLakeConnectionClient(adlsGen2Connection);
        DataLakeFileSystemClient fileSystemClient =
                client.getFileSystemClient(filesystem);
        DataLakeDirectoryClient directoryClient = fileSystemClient
                .getDirectoryClient(blobPath);
        PagedIterable<PathItem> pathItems = directoryClient.listPaths(false, false, null,
                Duration.of(adlsGen2Connection.getTimeout().longValue(), SECONDS));
        List<BlobInformations> blobs = new ArrayList<>();
        if (includeSubDirectory) {
            pathItems
                    .forEach(pathItem -> {
                        if (pathItem.isDirectory()) {
                            blobs.addAll(getBlobs(adlsGen2Connection, filesystem, pathItem.getName(), null, true));
                        }
                    });
        }
        blobs.addAll(pathItems
                .stream()
                .filter(pathItem -> !pathItem.isDirectory())
                .map(pathItem -> {
                    BlobInformations info = new BlobInformations();
                    info.setName(pathItem.getName());
                    info.setFileName(extractFileName(pathItem.getName()));
                    info.setBlobPath(pathItem.getName());
                    info.setDirectory(extractFolderPath(pathItem.getName()));
                    info.setExists(true);
                    info.setEtag(pathItem.getETag());
                    info.setContentLength(pathItem.getContentLength());
                    info.setLastModified(pathItem.getLastModified().toString());
                    return info;
                })
                .collect(Collectors.toList()));
        return blobs;
    }

    public DataLakeFileClient getDataLakeFileClient(AdlsGen2Connection adlsGen2Connection, String filesystem,
            String blobPath, String targetName) {
        DataLakeServiceClient client = getDataLakeConnectionClient(adlsGen2Connection);
        DataLakeFileSystemClient fileSystemClient =
                client.getFileSystemClient(filesystem);
        DataLakeDirectoryClient directoryClient = fileSystemClient
                .getDirectoryClient(blobPath);
        DataLakeFileClient fileClient = directoryClient.createFile(targetName, true);
        return fileClient;
    }

}
