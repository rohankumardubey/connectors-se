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
package org.talend.components.adlsgen2.input;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.components.common.formats.DeltaFormatOptions;
import org.talend.components.common.formats.csv.CSVFieldDelimiter;
import org.talend.components.common.formats.csv.CSVFormatOptionsWithSchema;
import org.talend.components.common.formats.csv.CSVRecordDelimiter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.adlsgen2")
class AdlsGen2InputTestIT extends AdlsGen2IntegrationTestBase {

    @Disabled
    // seems all the IT tests don't run in jenkins build as miss account keys now
    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void testDelta(String authmethod) throws IOException, ClassNotFoundException {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));

        dataSet.setConnection(connection);
        // dataSet.setFilesystem("csv");
        dataSet.setBlobPath("azuregen2tuj/deltadata");
        dataSet.setFormat(FileFormat.DELTA);
        dataSet.setDeltaConfiguration(new DeltaFormatOptions());

        inputConfiguration.setDataSet(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components()
                .component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(2, records.size());
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void csvBasicCase(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job
                .components()
                .component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(10000, records.size());
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void csvWithUserSchema(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        CSVFormatOptionsWithSchema csvConfig = new CSVFormatOptionsWithSchema();
        csvConfig.getCsvFormatOptions().setFieldDelimiter(CSVFieldDelimiter.SEMICOLON);
        csvConfig.getCsvFormatOptions().setRecordDelimiter(CSVRecordDelimiter.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.getCsvFormatOptions().setUseHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job
                .components()
                .component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(500, records.size());
        Record record = records.get(0);
        // schema should comply file header
        assertNotNull(record.getString("IdCustomer"));
        assertNotNull(record.getString("FirstName"));
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void csvWithInferHeaderFromRuntime(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        CSVFormatOptionsWithSchema csvConfig = new CSVFormatOptionsWithSchema();
        csvConfig.getCsvFormatOptions().setFieldDelimiter(CSVFieldDelimiter.SEMICOLON);
        csvConfig.getCsvFormatOptions().setRecordDelimiter(CSVRecordDelimiter.LF);
        csvConfig.setCsvSchema("");
        csvConfig.getCsvFormatOptions().setUseHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job
                .components()
                .component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(500, records.size());
        Record record = records.get(0);
        // schema should comply file header
        assertNotNull(record.getString("id"));
        assertNotNull(record.getString("Firstname"));
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void csvWithAutoGeneratedHeaders(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        CSVFormatOptionsWithSchema csvConfig = new CSVFormatOptionsWithSchema();
        csvConfig.getCsvFormatOptions().setFieldDelimiter(CSVFieldDelimiter.SEMICOLON);
        csvConfig.getCsvFormatOptions().setRecordDelimiter(CSVRecordDelimiter.LF);
        csvConfig.setCsvSchema("");
        csvConfig.getCsvFormatOptions().setUseHeader(false);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job
                .components()
                .component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        // header is a record
        assertEquals(501, records.size());
        Record record = records.get(0);
        // schema should comply "fieldN"
        assertNotNull(record.getString("field0"));
        assertNotNull(record.getString("field1"));
    }

}
