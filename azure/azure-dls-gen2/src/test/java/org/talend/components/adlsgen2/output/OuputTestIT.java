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
package org.talend.components.adlsgen2.output;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.common.formats.AvroFormatOptions;
import org.talend.components.common.formats.Encoding;
import org.talend.components.common.formats.csv.CSVFieldDelimiter;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVFormatOptionsWithSchema;
import org.talend.components.common.formats.csv.CSVRecordDelimiter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents("org.talend.components.adlsgen2")

@Disabled
public class OuputTestIT extends AdlsGen2IntegrationTestBase {

    CSVFormatOptionsWithSchema csvConfig;

    AdlsGen2DataSet outDs;

    @Service
    private RecordBuilderFactory factory;

    @BeforeEach
    void setConfiguration() {
        outDs = new AdlsGen2DataSet();
        outDs.setConnection(connection);
        outDs.setFilesystem(storageFs);
        //
        csvConfig = new CSVFormatOptionsWithSchema();
        csvConfig.setCsvFormatOptions(new CSVFormatOptions());
        csvConfig.getCsvFormatOptions().setFieldDelimiter(CSVFieldDelimiter.SEMICOLON);
        csvConfig.getCsvFormatOptions().setRecordDelimiter(CSVRecordDelimiter.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.getCsvFormatOptions().setUseHeader(true);
    }

    @Test
    public void fromCsvToAvro() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //

        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath(basePathOut + "avro");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=150";
        //
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToJson() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //

        outDs.setFormat(FileFormat.JSON);
        outDs.setBlobPath(basePathOut + "json");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=150";
        //
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToParquet() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //

        outDs.setFormat(FileFormat.PARQUET);
        outDs.setBlobPath(basePathOut + "parquet");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=250";
        //
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToCsv() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.CSV);
        outDs.setBlobPath(basePathOut + "csv");
        outDs.setCsvConfiguration(csvConfig);
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("IT-csv-2-csv-whdr-data2222-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=250";
        //
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToCsvQuoted() {
        csvConfig.setCsvSchema("");
        csvConfig.getCsvFormatOptions().setUseHeader(false);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.CSV);
        outDs.setBlobPath(basePathOut + "csv");
        csvConfig.getCsvFormatOptions().setUseHeader(true);
        csvConfig.setCsvSchema("");
        csvConfig.getCsvFormatOptions().setTextEnclosureCharacter("\"");
        csvConfig.getCsvFormatOptions().setEscapeCharacter("\\");
        csvConfig.getCsvFormatOptions().setFieldDelimiter(CSVFieldDelimiter.COMMA);
        outDs.setCsvConfiguration(csvConfig);
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("IT-csv-2-csv-whdr-QUOTED__-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=250";
        //
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    void testOutputNull() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroFormatOptions avroConfig = new AvroFormatOptions();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathOut + "avro-output-nulls");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath(basePathOut + "avro-output-nulls");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("avro-null-data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final int schemaSize = 9;
        final Schema.Builder schemaBuilder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
        final Schema schema = schemaBuilder
                .withEntry(this.buildEntry("nullStringColumn", Schema.Type.STRING))
                .withEntry(this.buildEntry("nullStringColumn2", Schema.Type.STRING))
                .withEntry(this.buildEntry("nullIntColumn", Schema.Type.INT))
                .withEntry(this.buildEntry("nullLongColumn", Schema.Type.LONG))
                .withEntry(this.buildEntry("nullFloatColumn", Schema.Type.FLOAT))
                .withEntry(this.buildEntry("nullDoubleColumn", Schema.Type.DOUBLE))
                .withEntry(this.buildEntry("nullBooleanColumn", Schema.Type.BOOLEAN))
                .withEntry(this.buildEntry("nullByteArrayColumn", Schema.Type.BYTES))
                .withEntry(this.buildEntry("nullDateColumn", Schema.Type.DATETIME))
                .build();
        Record testRecord = components
                .findService(RecordBuilderFactory.class)
                .newRecordBuilder(schema)
                .withString("nullStringColumn", null)
                .build();
        List<Record> testRecords = Collections.singletonList(testRecord);
        components.setInputData(testRecords);
        Job
                .components() //
                .component("in", "test://emitter") //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build()
                .run();
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "test://collector") //
                .connections() //
                .from("in") //
                .to("out") //
                .build()
                .run();
        List<Record> records = components.getCollectedData(Record.class);
        Record firstRecord = records.get(0);
        Assertions.assertEquals(schemaSize, firstRecord.getSchema().getEntries().size());
        Assertions.assertNull(firstRecord.getString("nullStringColumn"));
        Assertions.assertNull(firstRecord.getString("nullStringColumn2"));
        Assertions.assertNull(firstRecord.get(Integer.class, "nullIntColumn"));
        Assertions.assertNull(firstRecord.get(Long.class, "nullLongColumn"));
        Assertions.assertNull(firstRecord.get(Float.class, "nullFloatColumn"));
        Assertions.assertNull(firstRecord.get(Double.class, "nullDoubleColumn"));
        Assertions.assertNull(firstRecord.get(Boolean.class, "nullBooleanColumn"));
        Assertions.assertNull(firstRecord.get(byte[].class, "nullByteArrayColumn"));
        Assertions.assertNull(firstRecord.getDateTime("nullDateColumn"));
    }

    @Test
    void testSchemaIsNotMissingForNullsInFirstRecord() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroFormatOptions avroConfig = new AvroFormatOptions();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathOut + "avro-nulls");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath(basePathOut + "avro-nulls");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("avro-null-data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final int fieldSize = 2;
        Schema.Builder schemaBuilder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
        Schema schema = schemaBuilder
                .withEntry(this.buildEntry("stringColumn", Schema.Type.STRING))
                .withEntry(this.buildEntry("intColumn", Schema.Type.INT))
                .build();
        List<Record> testRecords = new ArrayList<>();
        testRecords
                .add(components
                        .findService(RecordBuilderFactory.class)
                        .newRecordBuilder(schema)
                        .withString("stringColumn", "a")
                        .build()); // stringColumn:a, intColumn:null
        testRecords
                .add(components
                        .findService(RecordBuilderFactory.class)
                        .newRecordBuilder(schema)
                        .withString("stringColumn", "b") //
                        .withInt("intColumn", Integer.MAX_VALUE) //
                        .build()); // stringColumn:a,
        // intColumn:not null
        components.setInputData(testRecords);
        Job
                .components() //
                .component("in", "test://emitter") //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build()
                .run();
        Job
                .components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "test://collector") //
                .connections() //
                .from("in") //
                .to("out") //
                .build()
                .run();
        List<Record> records = components.getCollectedData(Record.class);
        Assertions.assertEquals(fieldSize, records.get(0).getSchema().getEntries().size());
        Assertions.assertEquals(fieldSize, records.get(1).getSchema().getEntries().size());
    }

    @ParameterizedTest
    @ValueSource(strings = { "SJIS", "GB2312", "ISO-8859-1" })
    void outputToCsvEncoded(String encoding) {
        csvConfig = new CSVFormatOptionsWithSchema();
        csvConfig.getCsvFormatOptions().setRecordDelimiter(CSVRecordDelimiter.LF);
        csvConfig.getCsvFormatOptions().setEncoding(Encoding.OTHER);
        csvConfig.getCsvFormatOptions().setCustomEncoding(encoding);
        csvConfig.setCsvSchema("id;value");
        //
        String sample = "bb";
        String sampleA = "テスト";
        String sampleB = "电话号码";
        String sampleC = "cèt été, il va faïre bôt !";
        switch (encoding) {
        case "SJIS":
            sample = sampleA;
            break;
        case "GB2312":
            sample = sampleB;
            break;
        case "ISO-8859-1":
            sample = sampleC;
            break;
        default:
            fail("Should not be here for encoding:" + encoding);
        }
        //
        Schema schema = recordBuilderFactory
                .newSchemaBuilder(Schema.Type.RECORD) //
                .withEntry(this.buildEntry("id", Schema.Type.INT)) //
                .withEntry(this.buildEntry("value", Type.STRING)) //
                .build();
        List<Record> testRecords = new ArrayList<>();
        testRecords
                .add(recordBuilderFactory
                        .newRecordBuilder(schema) //
                        .withInt("id", 1) //
                        .withString("value", sample) //
                        .build());
        //
        String blobPath = String.format("%scsv-encoded-%s", basePathOut, encoding);
        //
        // now outputs
        //
        outDs.setFormat(FileFormat.CSV);
        outDs.setCsvConfiguration(csvConfig);
        outDs.setBlobPath(blobPath);
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate(String.format("csv-%s-encoded-data-", encoding));
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        components.setInputData(testRecords);
        Job
                .components() //
                .component("in", "test://emitter") //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build()
                .run();
        // now read back
        dataSet.setFormat(FileFormat.CSV);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(blobPath);
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job
                .components()
                .component("in", "Azure://AdlsGen2Input?" + config) //
                .component("out", "test://collector") //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertFalse(records.isEmpty());
        for (Record encoded : records) {
            assertNotNull(encoded);
            log.warn("[outputToCsvEncoded] {}", encoded);
            assertEquals("1", encoded.getString("id"));
            assertEquals(sample, encoded.getString("value"));
        }
    }

    private Schema.Entry buildEntry(final String name, final Schema.Type type) {
        return this.factory.newEntryBuilder().withType(type).withName(name).withNullable(true).build();
    }
}
