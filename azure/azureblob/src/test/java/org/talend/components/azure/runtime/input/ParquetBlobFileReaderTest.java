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
package org.talend.components.azure.runtime.input;

import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.azure.source.BlobInputProperties;
import org.talend.components.azure.source.BlobSource;
import org.talend.components.common.connection.azureblob.AzureAuthType;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionAccount;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.sdk.component.api.exception.ComponentException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

class ParquetBlobFileReaderTest {

    @Test
    void testADAuthNotSupportedForParquet() throws Exception {
        MessageService messageService = Mockito.mock(MessageService.class);
        Mockito.when(messageService.authTypeNotSupportedForParquet()).thenReturn("Some error message");
        Mockito.when(messageService.cantStartReadBlobItems(anyString()))
                .thenReturn("Parent error message for " + "Some error message");

        AzureCloudConnection connection = new AzureCloudConnection();
        AzureStorageConnectionAccount accountConnection = new AzureStorageConnectionAccount();
        accountConnection.setAuthType(AzureAuthType.ACTIVE_DIRECTORY_CLIENT_CREDENTIAL);
        connection.setAccountConnection(accountConnection);

        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(connection);
        dataset.setFileFormat(FileFormat.PARQUET);

        BlobInputProperties inputProperties = new BlobInputProperties();
        inputProperties.setDataset(dataset);

        AzureBlobComponentServices componentServices = Mockito.mock(AzureBlobComponentServices.class);
        AzureComponentServices azureConnectionServices = Mockito.mock(AzureComponentServices.class);
        CloudBlobClient blobClientMock = Mockito.mock(CloudBlobClient.class);
        CloudBlobContainer containerReferenceMock = Mockito.mock(CloudBlobContainer.class);
        Mockito.when(containerReferenceMock.exists()).thenReturn(true);
        Mockito.when(containerReferenceMock.listBlobs(any(), anyBoolean(), any(), any(), any()))
                .thenReturn(
                        Collections.singletonList(Mockito.mock(ListBlobItem.class)));

        Mockito.when(blobClientMock.getContainerReference(any())).thenReturn(containerReferenceMock);

        Mockito.when(azureConnectionServices.createCloudBlobClient(any(), any())).thenReturn(blobClientMock);

        Mockito.when(componentServices.getConnectionService()).thenReturn(azureConnectionServices);
        BlobSource reader = new BlobSource(inputProperties, componentServices, null, messageService);

        ComponentException exception = Assertions.assertThrows(ComponentException.class, reader::init);
        Assertions.assertTrue(exception.getMessage().contains(messageService.authTypeNotSupportedForParquet()),
                "Not expected exception message appeared");
    }
}