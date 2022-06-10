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
package org.talend.components.azure.common.connection;

import java.io.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.connection.azureblob.AzureAuthType;
import org.talend.components.common.connection.azureblob.AzureConnectionActiveDir;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionAccount;
import org.talend.components.common.connection.azureblob.Protocol;

class AzureStorageConnectionAccountTestIT {

    @Test
    void testSerial() throws IOException, ClassNotFoundException {
        final AzureStorageConnectionAccount account = new AzureStorageConnectionAccount();
        account.setAccountKey("mykey");
        account.setAccountName("MyAccountName");
        account.setAuthType(AzureAuthType.ACTIVE_DIRECTORY_CLIENT_CREDENTIAL);
        account.setProtocol(Protocol.HTTPS);
        account.setActiveDirProperties(new AzureConnectionActiveDir());
        account.getActiveDirProperties().setClientId("MyClientId");
        account.getActiveDirProperties().setClientSecret("MySecret");
        account.getActiveDirProperties().setTenantId("MyTenantId");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(account);

        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final AzureStorageConnectionAccount cnxCopy = (AzureStorageConnectionAccount) ois.readObject();
        Assertions.assertEquals(account, cnxCopy);

    }
}