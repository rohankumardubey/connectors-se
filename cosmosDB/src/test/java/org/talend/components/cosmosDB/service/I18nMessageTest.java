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
package org.talend.components.cosmosDB.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.service.http.ValidateSites;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.*;

@WithComponents(value = "org.talend.components.cosmosDB")
class I18nMessageTest {

    @Service
    private I18nMessage i18n;

    @Test
    void noResultFetched() {
        Assertions.assertNotNull(i18n.noResultFetched());
    }

    @Test
    void databaseNotExist() {
        final String msg = i18n.databaseNotExist("TheDB");
        Assertions.assertTrue(msg.contains("TheDB"), "Error on databaseNotExist msg");
    }

    @Test
    void destinationUnreachable() {
        Assertions.assertNotNull(i18n.destinationUnreachable());
    }

    @Test
    void connectionKODetailed() {
        final String msg = i18n.connectionKODetailed("Error123");
        Assertions.assertTrue(msg.contains("Error123"), "Error on connectionKODetailed msg");
    }

    @Test
    void vacantDBID() {
        Assertions.assertNotNull(i18n.vacantDBID());
    }

    @Test
    void connectionSuccess() {
        Assertions.assertNotNull(i18n.connectionSuccess());
    }

    @Test
    void notValidAddress() {
        ValidateSites.Environment env = (String globalName, String localName, String defaultValue) -> true;
        final String msg = ValidateSites.buildErrorMessage(i18n::notValidAddress, "http://1234", env);

        Assertions.assertTrue(msg.matches("^.*http://1234.*true.*true.*true.*$"));
    }
}