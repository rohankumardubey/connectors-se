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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.common.formats.csv.CSVFieldDelimiter;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVFormatOptionsWithSchema;
import org.talend.components.common.formats.csv.CSVRecordDelimiter;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.adlsgen2")
public class CSVInputIT extends AdlsGen2IntegrationTestBase {

    private InputConfiguration inputConfiguration;

    private String realFSName;

    @BeforeEach
    void initDataset() {
        dataSet.setFormat(FileFormat.CSV);
        dataSet.setBlobPath("someDir");
        realFSName = dataSet.getFilesystem();
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setRecordDelimiter(CSVRecordDelimiter.LF);
        CSVFormatOptionsWithSchema formatOptionsWithSchema = new CSVFormatOptionsWithSchema();
        formatOptionsWithSchema.setCsvFormatOptions(formatOptions);
        dataSet.setCsvConfiguration(formatOptionsWithSchema);
        inputConfiguration = new InputConfiguration();
        inputConfiguration.setDataSet(dataSet);
    }

    @Test
    void selectAllInputPipelineTest() throws Exception {
        final int recordSize = 10;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });

        createAndPopulateFileInStorage(inputConfiguration.getDataSet(), columns, recordSize);
        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
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
        Record firstRecord = records.get(0);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Assertions
                .assertEquals(columns.size(), firstRecord.getSchema().getEntries().size(),
                        "Columns number is different");
    }

    @Test
    void selectFromNotExistingDirectory() {
        inputConfiguration.getDataSet().setBlobPath("notExistingDir");
        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.ExecutorBuilder job = Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build();
        Assertions
                .assertThrows(ComponentException.class, job::run,
                        "Can't start reading blob items: Specified directory doesn't exist");
    }

    @Test
    void selectFromNotExistingFileSystem() {
        inputConfiguration.getDataSet().setFilesystem("notexistingcontainer");
        inputConfiguration.getDataSet().setBlobPath("notExistingDir");
        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.ExecutorBuilder job = Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build();
        Assertions
                .assertThrows(ComponentException.class, job::run,
                        "Can't start reading blob items: Specified container doesn't exist");
    }

    @Test
    void invalidFSNameInDataSet() {
        inputConfiguration.getDataSet().setFilesystem("inVaLiDcoNtAinErName");
        inputConfiguration.getDataSet().setBlobPath("notExistingDir");
        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.ExecutorBuilder job = Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build();
        Assertions
                .assertThrows(ComponentException.class, job::run,
                        "Can't start reading blob items: Container name is not valid");
    }

    @Test
    void testInputMultipleFiles() throws Exception {
        final int recordSize = 10 + 5;

        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        inputConfiguration.getDataSet().setBlobPath("someDir");
        createAndPopulateFileInStorage(inputConfiguration.getDataSet(), columns, 10);
        createAndPopulateFileInStorage(inputConfiguration.getDataSet(), columns, 5);

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
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

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testHeaderIsGreaterThanFileContent() throws Exception {
        final int recordSize = 0;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        inputConfiguration.getDataSet().setBlobPath("someDir");
        createAndPopulateFileInStorage(inputConfiguration.getDataSet(), columns, 1);
        inputConfiguration.getDataSet().getCsvConfiguration().getCsvFormatOptions().setUseHeader(true);
        inputConfiguration.getDataSet().getCsvConfiguration().getCsvFormatOptions().setHeader(5);

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
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

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testCorrectHeader() throws Exception {
        final int recordSize = 5 - 1;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        inputConfiguration.getDataSet().setBlobPath("someDir");
        createAndPopulateFileInStorage(inputConfiguration.getDataSet(), columns, 5);
        inputConfiguration.getDataSet().getCsvConfiguration().getCsvFormatOptions().setUseHeader(true);
        inputConfiguration.getDataSet().getCsvConfiguration().getCsvFormatOptions().setHeader(1);

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
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

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testReadFileFromRootDir() throws Exception {
        final int recordSize = 5;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        inputConfiguration.getDataSet().setBlobPath("/");

        createAndPopulateFileInStorage(inputConfiguration.getDataSet(), columns, 5);

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
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

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testReadCSVWithShortRecord() throws Exception {
        final int recordSize = 3;
        final int columnSize = 3;
        inputConfiguration.getDataSet()
                .getCsvConfiguration()
                .getCsvFormatOptions()
                .setFieldDelimiter(CSVFieldDelimiter.SEMICOLON);
        inputConfiguration.getDataSet().setBlobPath("csv");

        uploadTestFile("common/format/csv/csvWithShortRecord.csv", "csvWithShortRecord.csv");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
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

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Assertions
                .assertEquals(columnSize, records.get(0).getSchema().getEntries().size(),
                        "Columns number is different");
        Assertions
                .assertNull(records.get(2).getString("field2"),
                        "Value for last column in the short row should be null");

    }

    @AfterEach
    void resetFSName() {
        dataSet.setFilesystem(realFSName);
    }
}
