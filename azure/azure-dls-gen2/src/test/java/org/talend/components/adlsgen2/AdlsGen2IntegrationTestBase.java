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
package org.talend.components.adlsgen2;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.common.connection.adls.AuthMethod;
import org.talend.components.common.converters.CSVConverter;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

@WithComponents("org.talend.components.adlsgen2")
@WithMavenServers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdlsGen2IntegrationTestBase {

    @Injected
    protected BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @DecryptedServer("azure-dls-gen2.sas")
    private static Server mvnAccountSAS;

    @DecryptedServer("azure-dls-gen2.sharedkey")
    private static Server mvnAccountSharedKey;

    private static DataLakeServiceClient connectionClient;

    protected static AdlsGen2Connection connection;

    protected static AdlsGen2DataSet dataSet;

    protected static String accountName;

    protected static String storageFs;

    protected static String accountKey;

    protected static String sas;

    @BeforeAll
    public static void initConnection() {
        accountName = mvnAccountSAS.getUsername();
        storageFs = "test-it-" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
        accountKey = mvnAccountSharedKey.getPassword();
        sas = mvnAccountSAS.getPassword();

        Assume.assumeThat(accountName, Matchers.not("username"));
        Assume.assumeThat(sas, Matchers.not("password"));

        // can be reset in parametrized tests, but need to specify when creating ADLS FileSystem in BeforeEach
        connection = new AdlsGen2Connection();
        connection.setAuthMethod(AuthMethod.SharedKey);

        connection.setAccountName(accountName);
        connection.setSharedKey(accountKey);
        connection.setSas(sas);

        dataSet = new AdlsGen2DataSet();

        connectionClient = createTestADLSSharedKeyConnection(connection);
        connectionClient.createFileSystem(storageFs);

        dataSet.setConnection(connection);
        dataSet.setFilesystem(storageFs);
    }

    private static DataLakeServiceClient createTestADLSSharedKeyConnection(AdlsGen2Connection connection) {
        return new DataLakeServiceClientBuilder().credential(
                new StorageSharedKeyCredential(connection.getAccountName(), connection.getSharedKey()))
                .endpoint(connection.apiUrl())
                .buildClient();
    }

    protected void uploadTestFile(String from, String to) throws URISyntaxException {
        DataLakeFileSystemClient fsClient = connectionClient.getFileSystemClient(storageFs);

        File resourceFile = new File(AdlsGen2IntegrationTestBase.class.getClassLoader().getResource(from).toURI());
        String destinationDirName =
                dataSet.getBlobPath().endsWith("/") ? dataSet.getBlobPath() : dataSet.getBlobPath() + "/";
        DataLakeFileClient fileClient = fsClient.getFileClient(destinationDirName + to);
        fileClient.uploadFromFile(resourceFile.getPath());
    }

    protected void createAndPopulateFileInStorage(AdlsGen2DataSet fileOptions,
            List<String> recordSchema, int recordsSize) throws Exception {
        String fileName = "file" + RandomStringUtils.randomAlphabetic(5);
        String directory = fileOptions.getBlobPath();
        if (!directory.isEmpty() && !"/".equals(directory)) {
            fileName = directory + "/" + fileName;
        }
        DataLakeFileClient fileClient = connectionClient.getFileSystemClient(storageFs).getFileClient(fileName);

        byte[] content;
        if (fileOptions.getFormat() == FileFormat.CSV) {
            content = createCSVFileContent(recordsSize, recordSchema,
                    fileOptions.getCsvConfiguration().getCsvFormatOptions());
        } else {
            throw new IllegalStateException("Only CSV file format supported in this method");
        }
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content)) {
            fileClient.upload(inputStream, content.length);
        }
    }

    private byte[] createCSVFileContent(int recordsSize, List<String> columns, CSVFormatOptions formatOptions)
            throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));
        CSVFormat format = CSVConverter.of(recordBuilderFactory, formatOptions).getCsvFormat();

        CSVPrinter printer = new CSVPrinter(writer, format);

        for (int i = 0; i < recordsSize; i++) {
            List<String> values = new ArrayList<>();
            columns.forEach(column -> values.add(RandomStringUtils.randomAlphabetic(5)));

            printer.printRecord(values.toArray());
        }
        printer.flush();
        printer.close();
        return byteArrayOutputStream.toByteArray();
    }

    @AfterEach
    public void cleanUpADLSFileSystem() {
        DataLakeFileSystemClient fsClient = connectionClient.getFileSystemClient(storageFs);
        ListPathsOptions listPathsOptions = new ListPathsOptions();
        listPathsOptions.setRecursive(true);
        fsClient.listPaths(listPathsOptions, null).forEach(pathItem -> {
            if (!pathItem.isDirectory())
                fsClient.deleteFile(pathItem.getName());
        });
    }

    @AfterAll
    public void deleteFileSystem() {
        connectionClient.deleteFileSystem(storageFs);
    }
}
