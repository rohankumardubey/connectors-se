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
package org.talend.components.common.stream.input.excel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.talend.components.common.text.SchemaUtils;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class HTMLToRecord {

    private final RecordBuilderFactory recordBuilderFactory;

    private Supplier<String> notValidHTMLErrorMessageSupplier;

    @Deprecated
    public HTMLToRecord(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.notValidHTMLErrorMessageSupplier = () -> "File is not valid excel HTML"; // default one, shouldn't be used
    }

    public HTMLToRecord(RecordBuilderFactory recordBuilderFactory, Supplier<String> notValidHTMLErrorMessageSupplier) {
        this(recordBuilderFactory);
        this.notValidHTMLErrorMessageSupplier = notValidHTMLErrorMessageSupplier;
    }

    public Schema inferSchema(Element record) {
        List<String> columnNames = inferSchemaInfo(record, !isHeaderRecord(record));
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        columnNames
                .forEach(column -> schemaBuilder
                        .withEntry(recordBuilderFactory
                                .newEntryBuilder()
                                .withName(column)
                                .withType(Schema.Type.STRING)
                                .build()));
        return schemaBuilder.build();
    }

    public Record toRecord(Schema schema, Element record) {
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        final Elements rowColumns = record.getAllElements();
        if (rowColumns.size() > schema.getEntries().size() + 1) {
            throw new ComponentException(notValidHTMLErrorMessageSupplier.get());
        }
        for (int i = 1; i < rowColumns.size(); i++) {
            builder.withString(schema.getEntries().get(i - 1).getName(), rowColumns.get(i).text());
        }
        return builder.build();
    }

    private List<String> inferSchemaInfo(Element row, boolean useDefaultFieldName) {
        List<String> result = new ArrayList<>();
        Set<String> existNames = new HashSet<>();

        for (Element col : row.children()) { // skip first element since it would be the whole row
            String fieldName = col.ownText();
            if (useDefaultFieldName || fieldName == null || fieldName.length() == 0) {
                fieldName = "field" + col.elementSiblingIndex();
            }

            String finalName = SchemaUtils.correct(fieldName, col.elementSiblingIndex(), existNames);
            existNames.add(finalName);

            result.add(finalName);
        }
        return result;
    }

    private boolean isHeaderRecord(Element record) {
        return record.getElementsByTag("th").size() > 0;
    }
}
