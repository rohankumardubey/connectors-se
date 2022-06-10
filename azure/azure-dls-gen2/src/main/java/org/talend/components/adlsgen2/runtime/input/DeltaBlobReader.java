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
package org.talend.components.adlsgen2.runtime.input;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.components.common.Constants;
import org.talend.components.common.connection.adls.AuthMethod;
import org.talend.components.common.converters.DeltaConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeltaBlobReader extends BlobReader {

    public DeltaBlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            AdlsGen2Service connectionServices) {
        super(configuration, recordBuilderFactory, connectionServices);
    }

    @Override
    protected RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems) {
        return new DeltaRecordIterator(blobItems, recordBuilderFactory);
    }

    private class DeltaRecordIterator extends RecordIterator<RowRecord> {

        private DeltaConverter converter;

        private Configuration hadoopConfig;

        private CloseableIterator<RowRecord> iter;

        private RowRecord currentRecord;

        private DeltaRecordIterator(Iterable<BlobInformations> blobItemsList,
                RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            initConfig();
            peekFirstBlob();
        }

        private void initConfig() {
            hadoopConfig = new Configuration();

            AdlsGen2Connection datastore = configuration.getDataSet().getConnection();
            String accountName = datastore.getAccountName();
            AuthMethod authMethod = datastore.getAuthMethod();

            switch (authMethod) {
            case SharedKey:
                String sharedKey = datastore.getSharedKey();
                hadoopConfig.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "SharedKey");
                hadoopConfig.set("fs.azure.account.key." + accountName + ".dfs.core.windows.net", sharedKey);
                break;
            case SAS:
                // seems hadoop 3.2.2 don't support that, need to upgrade hadoop
                String sas = datastore.getSas();
                hadoopConfig.set("fs.azure.account.auth.type", "SAS");
                hadoopConfig.set("fs.azure.sas.token.provider.type",
                        "org.talend.components.adlsgen2.service.TalendSASTokenProvider");
                hadoopConfig.set(Constants.STATIC_SAS_TOKEN_KEY, sas.substring(1));
                break;
            case ActiveDirectory:
                String tenantId = datastore.getTenantId();
                String clientId = datastore.getClientId();
                String clientSecret = datastore.getClientSecret();
                hadoopConfig.set("fs.azure.account.auth.type", "OAuth");
                hadoopConfig.set("fs.azure.account.oauth.provider.type",
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
                hadoopConfig.set("fs.azure.account.oauth2.client.endpoint",
                        "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token");
                hadoopConfig.set("fs.azure.account.oauth2.client.id", clientId);
                hadoopConfig.set("fs.azure.account.oauth2.client.secret", clientSecret);
                break;
            default:
                break;
            }
        }

        @Override
        protected Record convertToRecord(RowRecord next) {
            if (converter == null) {
                converter = DeltaConverter.of(getRecordBuilderFactory());
            }

            return converter.toRecord(next);
        }

        @Override
        protected void readBlob() {
            closePreviousIterator();
            try {
                // as delta format is a directory self with parquet files and json files in it, so
                // we need to list blob objects? i think no need
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append("abfss://")
                        .append(configuration.getDataSet().getFilesystem())
                        .append('@')
                        .append(configuration.getDataSet().getConnection().getAccountName())
                        .append('.')
                        .append(configuration.getDataSet().getConnection().getEndpointSuffix());
                if (!getCurrentBlob().getBlobPath().startsWith("/")) {
                    strBuilder.append("/");
                }
                strBuilder.append(getCurrentBlob().getBlobPath());

                DeltaLog log = DeltaLog.forTable(hadoopConfig, strBuilder.toString());
                Snapshot snapshot = log.snapshot();
                iter = snapshot.open();

                this.currentRecord = nextRecord();
            } catch (Exception e) {
                log.error("[DeltaIterator] {}", e.getMessage());
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        protected boolean hasNextBlobRecord() {
            return currentRecord != null;
        }

        private RowRecord nextRecord() {
            if (iter.hasNext()) {
                RowRecord row = iter.next();
                return row;
            }

            return null;
        }

        @Override
        protected RowRecord peekNextBlobRecord() {
            RowRecord currentRecord = this.currentRecord;
            try {
                this.currentRecord = nextRecord();
            } catch (Exception e) {
                log.error("Can't read record from file " + getCurrentBlob().getBlobPath(), e);
            }

            return currentRecord;
        }

        @Override
        protected void complete() {
            closePreviousIterator();
        }

        private void closePreviousIterator() {
            if (iter != null) {
                try {
                    iter.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }

}
