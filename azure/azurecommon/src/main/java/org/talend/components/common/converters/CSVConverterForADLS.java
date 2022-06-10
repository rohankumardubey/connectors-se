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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.common.formats.csv.CSVFormatOptionsWithSchema;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVConverterForADLS extends CSVConverter implements Serializable {

    @Getter
    private CSVFormat csvFormat;

    private RecordBuilderFactory recordBuilderFactory;

    @Setter
    private Map<String, Integer> runtimeHeaders;

    private CSVConverterForADLS(final RecordBuilderFactory factory,
            final @Configuration("csvConfiguration") CSVFormatOptionsWithSchema configuration) {
        super(factory, configuration.getCsvFormatOptions());
        recordBuilderFactory = factory;
        csvFormat = formatWithConfiguration(configuration);
        schema = schemaWithConfiguration(configuration);
        log.debug("[CsvConverter] format: {}, schema: {}", csvFormat, schema);
    }

    public static CSVConverterForADLS of(final RecordBuilderFactory factory,
            final @Configuration("csvConfiguration") CSVFormatOptionsWithSchema configuration) {
        return new CSVConverterForADLS(factory, configuration);
    }

    private Schema schemaWithConfiguration(CSVFormatOptionsWithSchema configuration) {
        if (StringUtils.isEmpty(configuration.getCsvSchema())) {
            // will infer schema on runtime
            return null;
        }
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Set<String> existNames = new HashSet<>();
        int index = 0;
        for (String s : configuration.getCsvSchema()
                .split(String.valueOf(configuration.getCsvFormatOptions().effectiveFieldDelimiter()))) {
            Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            String finalName = RecordConverter.getCorrectSchemaFieldName(s, index++, existNames);
            existNames.add(finalName);
            builder.withEntry(entryBuilder.withName(finalName).withType(Schema.Type.STRING).withNullable(true).build());
        }

        return builder.build();
    }

    private CSVFormat
            formatWithConfiguration(@Configuration("csvConfiguration") final CSVFormatOptionsWithSchema configuration) {
        CSVFormat format = super.createCSVFormat(CSVFormat.DEFAULT, configuration.getCsvFormatOptions());
        String confSchema = configuration.getCsvSchema();
        String enclosure = configuration.getCsvFormatOptions().getTextEnclosureCharacter();

        if (StringUtils.isNotEmpty(enclosure) && enclosure.length() == 1) {
            format = format.withQuoteMode(QuoteMode.ALL);
        }

        // first line is header
        if (configuration.getCsvFormatOptions().isUseHeader()) {
            format = format.withFirstRecordAsHeader();
        }
        // header columns
        if (configuration.getCsvFormatOptions().isUseHeader() && StringUtils.isNotEmpty(confSchema)) {
            format = format
                    .withHeader(confSchema
                            .split(String.valueOf(configuration.getCsvFormatOptions().effectiveFieldDelimiter())));
        }

        return format;
    }

    @Override
    public Schema inferSchema(CSVRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Set<String> existNames = new HashSet<>();
        String finalName;
        int index = 0;
        // record.toMap() return an unsorted map, so will loose fields ordering.
        // see CsvIterator constructor.
        if (runtimeHeaders != null) {
            for (Entry<String, Integer> f : runtimeHeaders.entrySet()) {
                Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
                finalName = RecordConverter.getCorrectSchemaFieldName(f.getKey(), index++, existNames);
                existNames.add(finalName);
                builder
                        .withEntry(entryBuilder
                                .withName(finalName)
                                .withType(Schema.Type.STRING)
                                .withNullable(true)
                                .build());
            }
        } else {
            return super.inferSchema(record);
        }
        return builder.build();
    }

    @Override
    public Record toRecord(CSVRecord csvRecord) {
        return super.toRecord(csvRecord);
    }

    @Override
    public CSVRecord fromRecord(Record record) {
        throw new UnsupportedOperationException("#fromRecord()");
    }

}
