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
package org.talend.components.azure.migration;

import java.util.Map;
import org.talend.components.common.formats.Encoding;
import org.talend.sdk.component.api.component.MigrationHandler;

public class AzureStorageDatasetMigration implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        if (incomingVersion < 2) {
            String version1EncodingPathCSV = "csvOptions.encoding";
            String version1EncodingPathExcel = "excelOptions.encoding";
            migrateEncoding(incomingData, version1EncodingPathCSV, version1EncodingPathExcel);
        }
        return incomingData;
    }

    static void migrateEncoding(Map<String, String> incomingData, String version1EncodingPathCSV,
            String version1EncodingPathExcel) {
        if ("UFT8".equals(incomingData.get(version1EncodingPathCSV))) {
            incomingData.put(version1EncodingPathCSV, Encoding.UTF8.toString());
        }
        if ("UFT8".equals(incomingData.get(version1EncodingPathExcel))) {
            incomingData.put(version1EncodingPathExcel, Encoding.UTF8.toString());
        }
    }
}
