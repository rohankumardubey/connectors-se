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
package org.talend.components.common.converters;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.talend.components.common.stream.input.excel.ExcelToRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ExcelConverter implements RecordConverter<Row> {

    private ExcelToRecord excelToRecord;

    private RecordBuilderFactory builderFactory;

    private Sheet sheet;

    private ExcelConverter(RecordBuilderFactory builderFactory) {
        this.builderFactory = builderFactory;
        this.excelToRecord = new ExcelToRecord(builderFactory);
    }

    /**
     * Create excel converter for input
     *
     * @param builderFactory framework service should be injected
     * @return new instance of ExcelConverter could be used for input component
     */
    public static ExcelConverter of(RecordBuilderFactory builderFactory) {
        return new ExcelConverter(builderFactory);
    }

    /**
     * Create excel converter for output
     *
     * @param currentSheet sheet with rows
     * @return new instance of ExcelConverter could be used for output component
     */
    public static ExcelConverter ofOutput(Sheet currentSheet) {
        ExcelConverter converterForOutput = of(null);
        converterForOutput.sheet = currentSheet;

        return converterForOutput;
    }

    /**
     * @param record
     * @return
     */
    @Override
    public Schema inferSchema(Row record) {
        return excelToRecord.inferSchema(record, false);
    }

    public void inferSchemaNames(Row next, boolean isHeader) {
        excelToRecord.inferSchema(next, isHeader);
    }

    @Override
    public Record toRecord(Row record) {
        return excelToRecord.toRecord(record);
    }

    @Override
    public Row fromRecord(Record record) {
        return null;
    }

    public void appendBatchToTheSheet(List<Record> batch, int startRowNumber) {
        if (sheet == null) {
            throw new IllegalStateException("Excel converter wasn't initialized correctly");
        }

        for (int i = startRowNumber; i < batch.size() + startRowNumber; i++) {
            Row row = sheet.createRow(i);
            Record currentRecord = batch.get(i - startRowNumber);
            for (int j = 0; j < currentRecord.getSchema().getEntries().size(); j++) {
                Cell cell = row.createCell(j);
                String entityName = currentRecord.getSchema().getEntries().get(j).getName();
                switch (currentRecord.getSchema().getEntries().get(j).getType()) {
                case BOOLEAN:
                    cell.setCellType(CellType.BOOLEAN);
                    cell.setCellValue(currentRecord.getBoolean(entityName));
                    break;
                case DATETIME:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(Date.from(currentRecord.getDateTime(entityName).toInstant()));
                    break;
                case INT:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getInt(entityName));
                    break;
                case LONG:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getLong(entityName));
                    break;
                case FLOAT:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getFloat(entityName));
                    break;
                case DOUBLE:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getDouble(entityName));
                    break;
                case BYTES:
                    cell.setCellType(CellType.STRING);
                    cell.setCellValue(Arrays.toString(currentRecord.getBytes(entityName)));
                    break;
                default:
                    cell.setCellType(CellType.STRING);
                    cell
                            .setCellValue(String
                                    .valueOf(currentRecord
                                            .get(Object.class,
                                                    currentRecord.getSchema().getEntries().get(j).getName())));
                }
            }
        }
    }

    public List<String> inferRecordColumns(Record record) {
        List<String> columnNames = new ArrayList<>();
        record.getSchema().getEntries().forEach(entry -> columnNames.add(entry.getName()));
        return columnNames;
    }
}
