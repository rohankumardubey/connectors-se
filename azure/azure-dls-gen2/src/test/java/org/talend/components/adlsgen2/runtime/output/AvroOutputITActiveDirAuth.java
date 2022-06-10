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
package org.talend.components.adlsgen2.runtime.output;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.common.connection.adls.AuthMethod;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.adlsgen2")
@Ignore /* Fill the Active dir credentials and run manually */
public class AvroOutputITActiveDirAuth extends AdlsGen2IntegrationTestBase {

    private OutputConfiguration outputConfiguration;

    private final String testStringValue = "test";

    private final boolean testBooleanValue = true;

    private final long testLongValue = 0L;

    private final int testIntValue = 1;

    private final double testDoubleValue = 2.0;

    private final ZonedDateTime testDateValue = ZonedDateTime.now();

    private final byte[] bytes = new byte[] { 1, 2, 3 };

    @BeforeEach
    public void setUp() {
        connection.setAuthMethod(AuthMethod.ActiveDirectory);
        connection.setTenantId("__FILLME__");
        connection.setClientId("__FILLME__");
        connection.setClientSecret("__FILLME__");

        dataSet.setFormat(FileFormat.AVRO);
        dataSet.setBlobPath("avroTest/");

        outputConfiguration = new OutputConfiguration();
        outputConfiguration.setDataSet(dataSet);
        outputConfiguration.setBlobNameTemplate("avroFile-");
    }

    @Test
    void testOutput() {
        final int recordSize = 5;

        Record testRecord = componentsHandler
                .findService(RecordBuilderFactory.class)
                .newRecordBuilder()
                .withString("stringValue", testStringValue)
                .withBoolean("booleanValue", testBooleanValue)
                .withLong("longValue", testLongValue)
                .withInt("intValue", testIntValue)
                .withDouble("doubleValue", testDoubleValue)
                .withDateTime("dateValue", testDateValue)
                .withBytes("byteArray", bytes)
                .build();

        List<Record> testRecords = new ArrayList<>();
        for (int i = 0; i < recordSize; i++) {
            testRecords.add(testRecord);
        }
        componentsHandler.setInputData(testRecords);

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outputConfig += "&$configuration.$maxBatchSize=" + recordSize;
        Job
                .components()
                .component("inputFlow", "test://emitter")
                .component("outputComponent", "Azure://AdlsGen2Output?" + outputConfig)
                .connections()
                .from("inputFlow")
                .to("outputComponent")
                .build()
                .run();

        InputConfiguration inputProperties = new InputConfiguration();
        inputProperties.setDataSet(outputConfiguration.getDataSet());

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build()
                .run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size());
        Record firstRecord = records.get(0);
        Assertions.assertEquals(testRecord.getString("stringValue"), firstRecord.getString("stringValue"));
        Assertions.assertEquals(testRecord.getBoolean("booleanValue"), firstRecord.getBoolean("booleanValue"));
        Assertions.assertEquals(testRecord.getLong("longValue"), firstRecord.getLong("longValue"));
        Assertions.assertEquals(testRecord.getInt("intValue"), firstRecord.getInt("intValue"));
        Assertions.assertEquals(testRecord.getDouble("doubleValue"), firstRecord.getDouble("doubleValue"), 0.01);
        Assertions.assertEquals(testRecord.getDateTime("dateValue"), firstRecord.getDateTime("dateValue"));
        Assertions.assertArrayEquals(testRecord.getBytes("byteArray"), firstRecord.getBytes("byteArray"));
    }

}
