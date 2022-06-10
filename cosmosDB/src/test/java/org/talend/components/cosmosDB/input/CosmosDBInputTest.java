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
package org.talend.components.cosmosDB.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.cosmosDB.dataset.QueryDataset;
import org.talend.components.cosmosDB.datastore.CosmosDBDataStore;
import org.talend.components.cosmosDB.service.CosmosDBService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.io.*;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

@Environment(DirectRunnerEnvironment.class)
@WithComponents(value = "org.talend.components.cosmosDB")
class CosmosDBInputTest {

    @Service
    private CosmosDBService service;

    @Test
    void testSerial() throws Exception {
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final CosmosDBInputConfiguration cfg = new CosmosDBInputConfiguration();
        cfg.setJsonForceDouble(true);
        final QueryDataset dataset = new QueryDataset();
        cfg.setDataset(dataset);
        dataset.setCollectionID("collect1234");
        dataset.setQuery("The query");

        CosmosDBDataStore store = new CosmosDBDataStore();
        dataset.setDatastore(store);
        store.setDatabaseID("idDB");

        final CosmosDBInput input = new CosmosDBInput(cfg, service, factory);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(input);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(in);
        final CosmosDBInput inputCopy = (CosmosDBInput) ois.readObject();

        final Field field = CosmosDBInput.class.getDeclaredField("configuration");
        field.setAccessible(true);
        final CosmosDBInputConfiguration cfgCopy = (CosmosDBInputConfiguration) field.get(inputCopy);
        Assertions.assertNotNull(cfgCopy);
        Assertions.assertNotNull(cfgCopy.getDataset());
        Assertions.assertEquals(dataset, cfgCopy.getDataset());
    }
}