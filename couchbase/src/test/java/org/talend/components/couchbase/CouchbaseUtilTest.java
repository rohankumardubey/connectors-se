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
package org.talend.components.couchbase;

import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import com.couchbase.client.java.Cluster;

public abstract class CouchbaseUtilTest {

    protected static final String BUCKET_NAME = "Administrator";

    private static final String CLUSTER_USERNAME = "Administrator";

    private static final String CLUSTER_PASSWORD = "password";

    private static final CouchbaseContainer COUCHBASE_CONTAINER;

    protected final Cluster couchbaseCluster;

    protected final CouchbaseDataStore couchbaseDataStore;

    @Injected
    protected BaseComponentsHandler componentsHandler;

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    static {
        COUCHBASE_CONTAINER = new CouchbaseContainer("couchbase/server:7.0.2")
                .withBucket(new BucketDefinition(BUCKET_NAME))
                .withCredentials(CLUSTER_USERNAME, CLUSTER_PASSWORD)
                .withAnalyticsService();
        COUCHBASE_CONTAINER.start();

    }

    public CouchbaseUtilTest() {
        couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getConnectionString());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(CLUSTER_PASSWORD);

        couchbaseCluster = Cluster.connect(COUCHBASE_CONTAINER.getConnectionString(), CLUSTER_USERNAME,
                CLUSTER_PASSWORD);

    }

    protected String generateDocId(String prefix, int number) {
        return prefix + "_" + number;
    }

}
