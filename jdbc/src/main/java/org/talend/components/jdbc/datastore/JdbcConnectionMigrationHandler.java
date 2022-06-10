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
package org.talend.components.jdbc.datastore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.talend.sdk.component.api.component.MigrationHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcConnectionMigrationHandler implements MigrationHandler {

    private final static String migration_log = "JDBC Connection migration : ";

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        Map<String, String> migrated = new HashMap<>(incomingData);

        if (incomingVersion < 2) {
            to_2(migrated);
        }

        if (incomingVersion < 3) {
            to_3(migrated);
        }

        return migrated;
    }

    private void to_2(Map<String, String> incomingData) {
        incomingData.putIfAbsent("authenticationType", AuthenticationType.BASIC.name());
    }

    private void to_3(final Map<String, String> incomingData) {
        incomingData.putIfAbsent("setRawUrl", "true");
    }

}
