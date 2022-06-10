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

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.text.SchemaUtils;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVConverter implements RecordConverter<CSVRecord> {

    private final boolean isHeaderUsed;

    @Getter
    private CSVFormat csvFormat;

    @Getter
    protected Schema schema;

    public RecordBuilderFactory recordBuilderFactory;

    protected CSVConverter(RecordBuilderFactory recordBuilderFactory, CSVFormatOptions csvFormatOptions) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.isHeaderUsed = csvFormatOptions.isUseHeader();
        // CSVFormat.RFC4180 use " as quote and no escape char and "," as field
        // delimiter and only quote if quote is set and necessary
        this.csvFormat = createCSVFormat(CSVFormat.RFC4180, csvFormatOptions);
    }

    protected CSVFormat createCSVFormat(CSVFormat defaultFormat,
            @Configuration("csvConfiguration") final CSVFormatOptions configuration) {
        CSVFormat format = defaultFormat.withDelimiter(configuration.effectiveFieldDelimiter());

        Character textEnclosureCharacter = null;
        if (StringUtils.isNotEmpty(configuration.getTextEnclosureCharacter())) {
            textEnclosureCharacter = configuration.getTextEnclosureCharacter().charAt(0);
        }

        Character enclosureChar = null;
        if (StringUtils.isNotEmpty(configuration.getEscapeCharacter())) {
            enclosureChar = configuration.getEscapeCharacter().charAt(0);
        }

        // the with method return a new object, so have to assign back
        if (textEnclosureCharacter != null) {
            format = format.withQuote(textEnclosureCharacter);
        } else {
            format = format.withQuote(null);
        }

        if (enclosureChar != null) {
            format = format.withEscape(enclosureChar);
        }

        if (StringUtils.isNoneEmpty(configuration.effectiveRecordSeparator())) {
            format = format.withRecordSeparator(configuration.effectiveRecordSeparator());
        }

        return format;
    }

    public static CSVConverter of(RecordBuilderFactory recordBuilderFactory, CSVFormatOptions csvFormatOptions) {
        return new CSVConverter(recordBuilderFactory, csvFormatOptions);
    }

    @Override
    public Schema inferSchema(CSVRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Set<String> existNames = new HashSet<>();
        int index = 0;
        for (int i = 0; i < record.size(); i++) {
            Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            String fieldName = record.get(i);
            if (!isHeaderUsed || fieldName == null || fieldName.isEmpty()) {
                fieldName = "field" + i;
            }

            String finalName = SchemaUtils.correct(fieldName, index++, existNames);
            existNames.add(finalName);
            builder.withEntry(entryBuilder.withName(finalName).withType(Schema.Type.STRING).withNullable(true).build());
        }
        return builder.build();
    }

    @Override
    public Record toRecord(CSVRecord csvRecord) {
        if (schema == null) {
            schema = inferSchema(csvRecord);
        }

        if (csvRecord.size() < schema.getEntries().size()) {
            return fillShortCSVRecordWithNulls(csvRecord);
        } else {
            return fillStandardCSVRecord(csvRecord);
        }
    }

    private Record fillStandardCSVRecord(CSVRecord csvRecord) {
        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        String value;
        for (int i = 0; i < schema.getEntries().size(); i++) {
            try {
                value = csvRecord.get(i).isEmpty() ? null : csvRecord.get(i);
            } catch (ArrayIndexOutOfBoundsException e) {
                value = null;
            }
            recordBuilder.withString(schema.getEntries().get(i), value);
        }
        return recordBuilder.build();
    }

    private Record fillShortCSVRecordWithNulls(CSVRecord csvRecord) {
        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        String value;
        for (int i = 0; i < csvRecord.size(); i++) {
            try {
                value = csvRecord.get(i).isEmpty() ? null : csvRecord.get(i);
            } catch (ArrayIndexOutOfBoundsException e) {
                value = null;
            }
            recordBuilder.withString(schema.getEntries().get(i), value);
        }

        for (int i = csvRecord.size(); i < schema.getEntries().size(); i++) {
            recordBuilder.withString(schema.getEntries().get(i), null);
        }
        return recordBuilder.build();
    }

    @Override
    public CSVRecord fromRecord(Record record) {
        throw new UnsupportedOperationException("#fromRecord()");
    }
}
