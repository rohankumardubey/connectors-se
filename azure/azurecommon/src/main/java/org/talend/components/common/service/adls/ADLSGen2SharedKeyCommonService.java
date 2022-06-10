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
package org.talend.components.common.service.adls;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.talend.components.common.connection.adls.AdlsGen2Connection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.configuration.Configuration;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemItem;
import com.azure.storage.file.datalake.models.PathItem;
import lombok.extern.slf4j.Slf4j;
import static java.lang.System.nanoTime;

@Slf4j
@Service
public class ADLSGen2SharedKeyCommonService implements Serializable {

    public static final String AZURE_BLOB_CONNECTION_LIST = "AZURE_BLOB_CONNECTION_LIST";

    public static final String AZURE_DATALAKE_CONNECTION_LIST = "AZURE_DATALAKE_CONNECTION_LIST";

    // ADSL Gen2
    public static final String SQLDWH_WORKING_DIR = "SQLDWH-bulk-load/temp" + nanoTime();

    public static final String ACTION_SUGGESTION_PATHS = "ACTION_SUGGESTION_PATHS";

    private DataLakeServiceClient getDataLakeSharedKeyConnectionClient(String accountName,
            @Option("accountKey") final String accountKey) {
        String defaultApiUrl;
        AdlsGen2Connection connection = new AdlsGen2Connection();
        connection.setSharedKey(accountKey);
        connection.setAccountName(accountName);

        defaultApiUrl = connection.apiUrl();
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        builder = builder.credential(new StorageSharedKeyCredential(accountName, accountKey));

        return builder.endpoint(defaultApiUrl).buildClient();
    }

    private DataLakeServiceClient getDataLakeSharedKeyConnectionClient(AdlsGen2Connection connection) {
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        builder = builder.credential(
                new StorageSharedKeyCredential(connection.getAccountName(), connection.getSharedKey()));

        return builder.endpoint(connection.apiUrl()).buildClient();
    }

    public List<String> filesystemList(AdlsGen2Connection connection) {
        DataLakeServiceClient client =
                getDataLakeSharedKeyConnectionClient(connection);

        return client.listFileSystems()
                .stream()
                .map(FileSystemItem::getName)
                .collect(Collectors.toList());
    }

    public SuggestionValues filesystemList(@Option("accountName") String accountName,
            @Option("accountKey") final String accountKey) {
        DataLakeServiceClient client =
                getDataLakeSharedKeyConnectionClient(accountName, accountKey);

        return new SuggestionValues(true,
                client.listFileSystems()
                        .stream()
                        .map(
                                item -> new SuggestionValues.Item(item.getName(), item.getName()))
                        .collect(Collectors.toList()));
    }

    public List<String> pathList(@Option("adlsGen2Connection") final AdlsGen2Connection connection, String filesystem,
            boolean recursive, String path, boolean includeDirectories) throws URISyntaxException, InvalidKeyException {
        try {
            DataLakeDirectoryClient directoryClient =
                    getDataLakeSharedKeyConnectionClient(connection).getFileSystemClient(filesystem)
                            .getDirectoryClient(path);
            Stream<PathItem> itemsStream = directoryClient.listPaths(recursive, false, null, null).stream();
            if (!includeDirectories) {
                itemsStream = itemsStream.filter(pathItem -> !pathItem.isDirectory());
            }
            return itemsStream.map(PathItem::getName).collect(Collectors.toList());
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            return Collections.emptyList();
        }
    }

    @Suggestions(ACTION_SUGGESTION_PATHS)
    public SuggestionValues pathList(@Option("accountName") String accountName,
            @Option("accountKey") final String accountKey,
            String filesystem) {
        AdlsGen2Connection connection = new AdlsGen2Connection();
        connection.setSharedKey(accountKey);
        connection.setAccountName(accountName);
        List<String> paths;
        try {
            paths = pathList(connection, filesystem, false, SQLDWH_WORKING_DIR, true);
        } catch (URISyntaxException | InvalidKeyException e) {
            return new SuggestionValues();
        }
        return new SuggestionValues(true,
                paths.stream().map(e -> new SuggestionValues.Item(e, e)).collect(Collectors.toList()));
    }

    public void pathDelete(@Configuration("connection") final AdlsGen2Connection connection, String filesystem,
            String path) {
        try {
            getDataLakeSharedKeyConnectionClient(connection).getFileSystemClient(filesystem).deleteFile(path);
            log.info("Delete path " + path + " success.");
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
    }

    public void createPath(@Configuration("connection") final AdlsGen2Connection connection, String filesystem,
            String filePath) {
        try {
            getDataLakeSharedKeyConnectionClient(connection).getFileSystemClient(filesystem).createFile(filePath);
            log.info("Create path " + filePath + "success.");
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
    }

    public void appendPath(@Configuration("connection") final AdlsGen2Connection connection, String filesystem,
            String fullPath, Path tempFilePath) {
        try (InputStream inputStream = new FileInputStream(tempFilePath.toFile())) {
            DataLakeFileClient fileClient =
                    getDataLakeSharedKeyConnectionClient(connection).getFileSystemClient(filesystem)
                            .getFileClient(fullPath);

            fileClient.append(inputStream, 0, Files.size(tempFilePath));
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
    }

    public void flushPath(@Configuration("connection") final AdlsGen2Connection connection, String filesystem,
            String fullPath, long fileLength) {
        try {
            DataLakeFileClient fileClient =
                    getDataLakeSharedKeyConnectionClient(connection).getFileSystemClient(filesystem)
                            .getFileClient(fullPath);
            fileClient.flush(fileLength);
            log.info("Flush path " + fullPath + " success.");
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
    }
}