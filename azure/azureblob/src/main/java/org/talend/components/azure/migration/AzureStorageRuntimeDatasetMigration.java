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

import org.talend.sdk.component.api.component.MigrationHandler;

import java.util.Map;

/**
 * This class is a workaround to migrate dataset encoding on pipelines until AzureStorageDatasetMigration
 * is not executed for runtime
 * (https://jira.talendforge.org/browse/TPRUN-486)
 */
public class AzureStorageRuntimeDatasetMigration implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        if (incomingVersion < 2) {
            String version1EncodingPathCSV = "configuration.dataset.csvOptions.encoding";
            String version1EncodingPathExcel = "configuration.dataset.excelOptions.encoding";
            AzureStorageDatasetMigration.migrateEncoding(incomingData, version1EncodingPathCSV,
                    version1EncodingPathExcel);
        }
        return incomingData;
    }
}
