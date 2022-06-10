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
package org.talend.components.adlsgen2.migration;

import java.util.HashMap;
import java.util.Map;
import org.talend.components.common.formats.csv.CSVFieldDelimiter;
import org.talend.sdk.component.api.component.MigrationHandler;

public class AdlsDataSetMigrationHandler implements MigrationHandler {

    private static final String DEFAULT_HEADER_SIZE = "1";

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        Map<String, String> migratedConfiguration = new HashMap<>(incomingData);
        if (incomingVersion < 2) {
            migrateDataset(migratedConfiguration, "");
        }
        if (incomingVersion < 3) {
            migrateCSVFieldDelimiterTabulation(migratedConfiguration,
                    "csvConfiguration.csvFormatOptions.fieldDelimiter");
        }
        return migratedConfiguration;
    }

    private static void putIfNotNull(Map<String, String> configMap, String from, String to) {
        if (configMap.containsKey(from) && configMap.get(from) != null) {
            configMap.put(to, configMap.remove(from));
        }
    }

    static void migrateDataset(Map<String, String> migratedConfiguration, String configPrefix) {
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.recordSeparator",
                configPrefix + "csvConfiguration.csvFormatOptions.recordDelimiter");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.customRecordSeparator",
                configPrefix + "csvConfiguration.csvFormatOptions.customRecordDelimiter");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.header",
                configPrefix + "csvConfiguration.csvFormatOptions.useHeader");
        migratedConfiguration.put(configPrefix + "csvConfiguration.csvFormatOptions.header", DEFAULT_HEADER_SIZE);
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.fileEncoding",
                configPrefix + "csvConfiguration.csvFormatOptions.encoding");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.customFileEncoding",
                configPrefix + "csvConfiguration.csvFormatOptions.customEncoding");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.fieldDelimiter",
                configPrefix + "csvConfiguration.csvFormatOptions.fieldDelimiter");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.customFieldDelimiter",
                configPrefix + "csvConfiguration.csvFormatOptions.customFieldDelimiter");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.textEnclosureCharacter",
                configPrefix + "csvConfiguration.csvFormatOptions.textEnclosureCharacter");
        putIfNotNull(migratedConfiguration, configPrefix + "csvConfiguration.escapeCharacter",
                configPrefix + "csvConfiguration.csvFormatOptions.escapeCharacter");
    }

    static void migrateCSVFieldDelimiterTabulation(Map<String, String> migratedConfiguration,
            String fieldDelimiterConfigPath) {
        if ("TABULATION".equals(migratedConfiguration.get(fieldDelimiterConfigPath))) {
            migratedConfiguration.put(fieldDelimiterConfigPath, CSVFieldDelimiter.TAB.toString());
        }
    }
}
