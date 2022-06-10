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
package org.talend.components.couchbase.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.configuration.ConnectionConfiguration;
import org.talend.components.couchbase.configuration.ConnectionParameter;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.Cluster;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseService class")
class CouchbaseServiceTest extends CouchbaseUtilTest {

    @Service
    private CouchbaseService couchbaseService;

    @Test
    @DisplayName("Test successful connection")
    void couchbaseSuccessfulConnectionTest() {
        assertEquals(HealthCheckStatus.Status.OK, couchbaseService.healthCheck(couchbaseDataStore).getStatus());
    }

    @Test
    @DisplayName("Test connection with wrong password")
    void couchbaseWrongPasswordNotSuccessfulConnectionTest() {
        String wrongPassword = "wrongpass";

        CouchbaseDataStore couchbaseDataStoreWrongPass = new CouchbaseDataStore();
        couchbaseDataStoreWrongPass.setBootstrapNodes(couchbaseDataStore.getBootstrapNodes());
        couchbaseDataStoreWrongPass.setUsername(couchbaseDataStore.getUsername());
        couchbaseDataStoreWrongPass.setPassword(wrongPassword);

        assertEquals(HealthCheckStatus.Status.KO,
                couchbaseService.healthCheck(couchbaseDataStoreWrongPass).getStatus());
    }

    @Test
    @DisplayName("Test connection to unavailable server")
    void couchbaseWrongAddressNotSuccessfulConnectionTest() {
        String wrongUrl = "192.168.0.101";

        CouchbaseDataStore couchbaseDataStoreWrongUrl = new CouchbaseDataStore();
        couchbaseDataStoreWrongUrl.setBootstrapNodes(wrongUrl);
        couchbaseDataStoreWrongUrl.setUsername(couchbaseDataStore.getUsername());
        couchbaseDataStoreWrongUrl.setPassword(couchbaseDataStore.getPassword());

        assertEquals(HealthCheckStatus.Status.KO,
                couchbaseService.healthCheck(couchbaseDataStoreWrongUrl).getStatus());
    }

    @Test
    @DisplayName("Test connection to non-existing bucket")
    void missingBucketNotSuccessfulConnectionTest() {
        String wrongBucket = "missing-bucket";

        try {
            couchbaseService.openDefaultCollection(couchbaseCluster, wrongBucket);
            assertTrue(false);
        } catch (ComponentException e) {
            assertEquals(
                    "(com.couchbase.client.core.error.BucketNotFoundException) Bucket [" + wrongBucket + "] not found.",
                    e.getMessage());
        }
    }

    @Test
    @DisplayName("Two bootstrap nodes without spaces")
    void resolveAddressesTest() {
        String inputUrl = "192.168.0.1,192.168.0.2";
        String[] resultArrayWithUrls = couchbaseService.resolveAddresses(inputUrl);
        assertEquals("192.168.0.1", resultArrayWithUrls[0], "first expected node");
        assertEquals("192.168.0.2", resultArrayWithUrls[1], "second expected node");
    }

    @Test
    @DisplayName("Two bootstrap nodes with extra spaces")
    void resolveAddressesWithSpacesTest() {
        String inputUrl = " 192.168.0.1, 192.168.0.2";
        String[] resultArrayWithUrls = couchbaseService.resolveAddresses(inputUrl);
        assertEquals("192.168.0.1", resultArrayWithUrls[0], "first expected node");
        assertEquals("192.168.0.2", resultArrayWithUrls[1], "second expected node");
    }

    @Test
    @DisplayName("Test setting custom timeout values")
    void customTimeoutValuesTest() {
        List<ConnectionConfiguration> timeouts = new ArrayList<>();
        timeouts.add(new ConnectionConfiguration(ConnectionParameter.CONNECTION_TIMEOUT, "30000"));
        timeouts.add(new ConnectionConfiguration(ConnectionParameter.QUERY_TIMEOUT, "80000"));
        timeouts.add(new ConnectionConfiguration(ConnectionParameter.ANALYTICS_TIMEOUT, "100000"));
        CouchbaseDataStore timeoutDatastore = new CouchbaseDataStore();
        timeoutDatastore.setBootstrapNodes(couchbaseDataStore.getBootstrapNodes());
        timeoutDatastore.setUsername(couchbaseDataStore.getUsername());
        timeoutDatastore.setPassword(couchbaseDataStore.getPassword());
        timeoutDatastore.setUseConnectionParameters(true);
        timeoutDatastore.setConnectionParametersList(timeouts);

        Cluster timeoutCluster = couchbaseService.openConnection(timeoutDatastore);
        timeoutCluster.waitUntilReady(Duration.ofSeconds(5));
        TimeoutConfig timeoutConfig = timeoutCluster.environment().timeoutConfig();
        assertEquals(30, timeoutConfig.connectTimeout().getSeconds());
        assertEquals(80, timeoutConfig.queryTimeout().getSeconds());
        assertEquals(100, timeoutConfig.analyticsTimeout().getSeconds());
    }

    @Test
    @DisplayName("Test setting wrong timeout values")
    void wrongTimeoutValuesTest() {
        List<ConnectionConfiguration> timeouts = new ArrayList<>();
        timeouts.add(new ConnectionConfiguration(ConnectionParameter.CONNECTION_TIMEOUT, "value"));
        CouchbaseDataStore timeoutDatastore = new CouchbaseDataStore();
        timeoutDatastore.setBootstrapNodes(couchbaseDataStore.getBootstrapNodes());
        timeoutDatastore.setUsername(couchbaseDataStore.getUsername());
        timeoutDatastore.setPassword(couchbaseDataStore.getPassword());
        timeoutDatastore.setUseConnectionParameters(true);
        timeoutDatastore.setConnectionParametersList(timeouts);

        try {
            Cluster timeoutCluster = couchbaseService.openConnection(timeoutDatastore);
            timeoutCluster.waitUntilReady(Duration.ofSeconds(5));
            assertTrue(false);
        } catch (ComponentException e) {
            assertEquals("Unexpected value: value. Only numerical values are accepted.", e.getMessage());
        }
    }

}
