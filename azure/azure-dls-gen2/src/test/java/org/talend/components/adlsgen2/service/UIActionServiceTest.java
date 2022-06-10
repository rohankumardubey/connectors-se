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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.common.connection.adls.AuthMethod;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import static org.mockito.ArgumentMatchers.any;

import com.azure.storage.file.datalake.models.DataLakeStorageException;

class UIActionServiceTest {

    @Mock
    private AdlsGen2Service service;

    @Mock
    private I18n i18n;

    @InjectMocks
    private UIActionService uiActionService = new UIActionService();

    @BeforeEach
    protected void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void filesystemListSuggestions() {
        List<String> expectedList = new ArrayList<>();
        expectedList.add("fs1");
        expectedList.add("fs2");
        Mockito.when(service.filesystemList(any())).thenReturn(expectedList);

        AdlsGen2Connection someConnection = new AdlsGen2Connection();
        someConnection.setAuthMethod(AuthMethod.SharedKey);
        someConnection.setAccountName("someAccount");
        someConnection.setSharedKey("someKey");
        SuggestionValues resultValues = uiActionService.filesystemList(someConnection);

        List<String> resultList =
                resultValues.getItems().stream().map(SuggestionValues.Item::getId).collect(Collectors.toList());

        Assertions.assertEquals(expectedList.size(), resultList.size());
        Assertions.assertArrayEquals(expectedList.toArray(new String[] {}), resultList.toArray(new String[] {}));
    }

    @Test
    void testHealthCheck() {
        Mockito.when(service.filesystemList(any())).thenReturn(Collections.singletonList("SomeFS"));

        AdlsGen2Connection someConnection = new AdlsGen2Connection();
        someConnection.setAuthMethod(AuthMethod.SharedKey);
        someConnection.setAccountName("someAccount");
        someConnection.setSharedKey("someKey");
        HealthCheckStatus resultStatus = uiActionService.validateConnection(someConnection);

        Assertions.assertEquals(HealthCheckStatus.Status.OK, resultStatus.getStatus());
    }

    @Test
    void testHealthCheckOKDespitePermissionIssueForAD() {
        DataLakeStorageException permissionIssueRuntimeException = Mockito.mock(DataLakeStorageException.class);
        Mockito.when(permissionIssueRuntimeException.getStatusCode()).thenReturn(403);
        Mockito.when(service.filesystemList(any())).thenThrow(permissionIssueRuntimeException);

        AdlsGen2Connection someConnection = new AdlsGen2Connection();
        someConnection.setAuthMethod(AuthMethod.ActiveDirectory);
        someConnection.setAccountName("someAccount");
        someConnection.setTenantId("someTenantID");
        someConnection.setClientId("someClientID");
        someConnection.setClientSecret("someClientSecret");
        final HealthCheckStatus status = this.uiActionService.validateConnection(someConnection);
        Assertions.assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
        Mockito.verify(i18n).healthCheckActiveDirectoryPermissions();
    }

    @Test
    void testHealthCheckKOForPermissionIssueForSAS() {
        Mockito.when(service.filesystemList(any())).thenThrow(ComponentException.class);

        AdlsGen2Connection someConnection = new AdlsGen2Connection();
        someConnection.setAuthMethod(AuthMethod.SAS);
        someConnection.setAccountName("someAccount");
        someConnection.setSas("?sv=2022-12-31&ss=bfqt&srt=sco&spr=https&sig=TEST_SHARED_SIGNATURE");
        final HealthCheckStatus status = this.uiActionService.validateConnection(someConnection);
        Assertions.assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

}
