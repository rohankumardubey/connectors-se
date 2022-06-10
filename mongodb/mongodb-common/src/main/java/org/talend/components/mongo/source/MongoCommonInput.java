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
package org.talend.components.mongo.source;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.mongo.service.DocumentToRecord;
import org.talend.components.mongo.service.MongoCommonService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MongoCommonInput implements Serializable {

    protected MongoCommonSourceConfiguration configuration;

    protected MongoCommonService service;

    protected RecordBuilderFactory builderFactory;

    protected transient JsonToRecord jsonToRecord;

    protected transient DocumentToRecord documentToRecord;

    protected Record doConvert(Document document) {
        switch (configuration.getDataset().getMode()) {
        case TEXT:
            return toRecordWithWSingleDocumentContentColumn(document);
        // case MAPPING:
        // return toFlatRecordWithMapping(document);
        case JSON:
        default:
            return convertDocument2RecordDirectly(document);
        }
    }

    protected Record convertDocument2Record(Document document) {
        // TODO bson can convert to json with loss data? check it
        String jsonContnt = document2Json(document);
        // can't use org.talend.components.common.stream.input.json.JsonRecordReader here, please see
        // org.talend.components.common.stream.input.json.JsonRecordReaderTest that is not the result what we expect
        // here
        // here we expect one document, one record always
        Record result = jsonToRecord.toRecord(getJsonObject(jsonContnt));
        return result;
    }

    protected Record convertDocument2RecordDirectly(Document document) {
        return documentToRecord.toRecord(document);
    }

    // TODO check it
    protected JsonObject getJsonObject(String jsonContent) {
        try (JsonReader reader = Json.createReader(new StringReader(jsonContent))) {
            return reader.readObject();
        }
    }

    protected String document2Json(Document document) {
        // http://mongodb.github.io/mongo-java-driver/3.12/bson/extended-json/
        // https://github.com/mongodb/specifications/blob/master/source/extended-json.rst
        // http://mongodb.github.io/mongo-java-driver/3.12/bson/documents/
        return document.toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build(), new DocumentCodec());
    }

    protected Record toRecordWithWSingleDocumentContentColumn(Document document) {
        Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);

        String singleColumnName = configuration.getDataset().getCollection();
        Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        entryBuilder.withNullable(true).withName(singleColumnName).withType(Schema.Type.STRING);
        Schema.Entry singleEntry = entryBuilder.build();
        schemaBuilder.withEntry(singleEntry);

        Schema schemaWithSingleColumn = schemaBuilder.build();

        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schemaWithSingleColumn);
        addColumn(recordBuilder, singleEntry, document);
        return recordBuilder.build();
    }

    protected void addColumn(Record.Builder recordBuilder, final Schema.Entry entry, Object value) {
        final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        Schema.Type type = entry.getType();
        entryBuilder.withName(entry.getName()).withNullable(true).withType(type);

        if (value == null) {
            // TODO check if it is right, when null, no need to fill something in the record?
            return;
        }

        switch (type) {
        case ARRAY:
            // TODO copy from couchbase connector, no use now, keep it for future, maybe not necessary
            Schema elementSchema = entry.getElementSchema();
            entryBuilder.withElementSchema(elementSchema);
            if (elementSchema.getType() == Schema.Type.RECORD) {
                List<Record> recordList = new ArrayList<>();
                // schema of the first element
                Schema currentSchema = elementSchema.getEntries().get(0).getElementSchema();
                for (int i = 0; i < ((List) value).size(); i++) {
                    Document currentJsonObject = (Document) ((List) value).get(i);
                    recordList.add(createRecord(currentSchema, currentJsonObject));
                }
                recordBuilder.withArray(entryBuilder.build(), recordList);
            } else {
                recordBuilder.withArray(entryBuilder.build(), ((List) value));
            }
            break;
        case FLOAT:
            recordBuilder.withFloat(entryBuilder.build(), (Float) value);
            break;
        case DOUBLE:
            recordBuilder.withDouble(entryBuilder.build(), (Double) value);
            break;
        case BYTES:
            recordBuilder.withBytes(entryBuilder.build(), (byte[]) value);
        case STRING:
            // toString is right for all type, like document? TODO
            recordBuilder
                    .withString(entryBuilder.build(),
                            value instanceof Document ? document2Json((Document) value) : value.toString());
            break;
        case LONG:
            recordBuilder.withLong(entryBuilder.build(), (Long) value);
            break;
        case INT:
            recordBuilder.withInt(entryBuilder.build(), (Integer) value);
            break;
        case DATETIME:
            recordBuilder.withDateTime(entryBuilder.build(), (Date) value);
            break;
        case BOOLEAN:
            recordBuilder.withBoolean(entryBuilder.build(), (Boolean) value);
            break;
        case RECORD:
            // TODO support it in future, maybe not necessary
            entryBuilder.withElementSchema(entry.getElementSchema());
            recordBuilder.withRecord(entryBuilder.build(), createRecord(entry.getElementSchema(), (Document) value));
            break;
        }
    }

    protected Record createRecord(Schema schema, Document document) {
        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
        schema.getEntries().forEach(entry -> addColumn(recordBuilder, entry, getValue(entry.getName(), document)));
        return recordBuilder.build();
    }

    protected Object getValue(String currentName, Document document) {
        if (document == null) {
            return null;
        }
        return document.get(currentName);
    }
}
