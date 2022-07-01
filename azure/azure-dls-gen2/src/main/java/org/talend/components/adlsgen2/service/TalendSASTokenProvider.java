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
package org.talend.components.adlsgen2.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.talend.components.common.Constants;

/*
 * Used as an implementation for SASTokenProvider in DeltaBlobReader fs.azure.sas.token.provider.type hadoop config
 */
public class TalendSASTokenProvider implements SASTokenProvider {

    Configuration configuration;

    @Override
    public void initialize(Configuration configuration, String accountName) {
        this.configuration = configuration;
    }

    @Override
    public String getSASToken(String accountName, String filesystem, String path, String operation) {
        return configuration.get(Constants.STATIC_SAS_TOKEN_KEY);
    }
}
