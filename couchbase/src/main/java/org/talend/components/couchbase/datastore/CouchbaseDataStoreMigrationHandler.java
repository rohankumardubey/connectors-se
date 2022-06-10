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
package org.talend.components.couchbase.datastore;

import java.util.Map;
import java.util.regex.Pattern;

import org.talend.components.couchbase.configuration.ConnectionParameter;
import org.talend.sdk.component.api.component.MigrationHandler;

public class CouchbaseDataStoreMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        if (incomingVersion < 2) {
            long timeoutValue = Long.parseLong(incomingData.get("connectTimeout")) * 1000;
            incomingData.remove("connectTimeout");
            incomingData.put("useConnectionParameters", "true");
            incomingData.put("connectionParametersList[0].parameterName",
                    ConnectionParameter.CONNECTION_TIMEOUT.name());
            incomingData.put("connectionParametersList[0].parameterValue", String.valueOf(timeoutValue));
        }

        if (incomingVersion < 3) {
            String pattern = "connectionParametersList[.*].parameterName";
            for (Map.Entry<String, String> entry : incomingData.entrySet()) {
                if (Pattern.matches(pattern, entry.getKey()) && entry.getValue().equals("MAX_REQUEST_LIFETIME")) {
                    incomingData.put(entry.getKey(), ConnectionParameter.QUERY_THRESHOLD.name());
                }
            }
        }
        return incomingData;
    }

}
