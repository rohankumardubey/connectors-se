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
package org.talend.components.mongodb.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.mongo.service.MongoCommonService;
import org.talend.components.mongodb.dataset.MongoDBReadDataSet;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.source.MongoDBQuerySourceConfiguration;
import org.talend.components.mongodb.source.MongoDBReader;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

@Version(1)
@Slf4j
@Service
public class MongoDBService extends MongoCommonService {

    @Service
    protected I18nMessage i18n;

    @HealthCheck("healthCheck")
    public HealthCheckStatus healthCheck(@Option("configuration.dataset.connection") final MongoDBDataStore datastore) {
        return super.healthCheck(datastore);
    }

    public Schema retrieveSchema(@Option("dataset") final MongoDBReadDataSet dataset) {
        MongoDBQuerySourceConfiguration configuration = new MongoDBQuerySourceConfiguration();
        configuration.setDataset(dataset);
        MongoDBReader reader = new MongoDBReader(configuration, this, super.builderFactory, i18n, null);
        reader.init();
        Record record = reader.next();
        reader.release();

        return record.getSchema();
    }

}