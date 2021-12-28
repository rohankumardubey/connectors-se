/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.docdb.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.docdb.dataset.DocDBDataSet;
import org.talend.components.docdb.datastore.DocDBDataStore;
import org.talend.components.docdb.service.DocDBConnectionService;
import org.talend.components.docdb.service.DocumentToRecord;
import org.talend.components.docdb.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import org.talend.components.docdb.service.DocDBService;

@Slf4j
@Documentation("This component reads data from DocDB")
public class DocDBInputSource implements Serializable {

    private I18nMessage i18nMessage;

    private static final transient Logger LOG = LoggerFactory.getLogger(DocDBInputSource.class);

    private final DocDBInputMapperConfiguration configuration;

    private final DocDBService service;

    private final RecordBuilderFactory builderFactory;

    private final DocDBConnectionService connectionService;

    private transient JsonToRecord jsonToRecord;

    private transient MongoClient client;

    private transient DocumentToRecord documentToRecord;

    private transient String query4Split;

    @Connection
    private transient DocDBConnectionService conn;

    public DocDBInputSource(@Option("configuration") final DocDBInputMapperConfiguration configuration,
            final DocDBService service,
            final RecordBuilderFactory builderFactory,
            final DocDBConnectionService connectionService,
            final I18nMessage i18nMessage,
            String query4Split) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.connectionService = connectionService;
        this.i18nMessage = i18nMessage;
        this.query4Split = query4Split;
    }

    Iterator<Document> iterator = null;

    @PostConstruct
    public void init() {
        jsonToRecord = new JsonToRecord(this.builderFactory);
        documentToRecord = new DocumentToRecord(this.builderFactory);

        DocDBDataSet dataSet = configuration.getDataSet();
        DocDBDataStore datastore = dataSet.getDataStore();
        client = getConnService().createClient(datastore);
        MongoDatabase database = client.getDatabase(datastore.getDataBase());
        MongoCollection<Document> collection = database.getCollection(dataSet.getCollection());

        iterator = fetchData(dataSet, collection);
    }

    private Iterator<Document> fetchData(DocDBDataSet dataSet, MongoCollection<Document> collection) {
        if (query4Split != null) {
            log.info("query for mongodb split : " + query4Split);
            return collection.find(service.getBsonDocument(query4Split)).iterator();
        }

        Long sampleLimit = configuration.getSampleLimit();
        if (dataSet.isUseQuery() == true) {
            // return fetchData((MongoDBReadDataSet) dataset, collection);
            BsonDocument query = service.getBsonDocument(dataSet.getQuery());
            FindIterable<Document> fi = collection.find(query);
            if (sampleLimit != null && sampleLimit > 0) {
                fi = fi.limit(sampleLimit.intValue());
            }
            return fi.iterator();
        } else {
            FindIterable<Document> fi = collection.find();
            if (sampleLimit != null && sampleLimit > 0) {
                fi = fi.limit(sampleLimit.intValue());
            }
            return fi.iterator();
        }
    }

    @Producer
    public Record next() {
        if (iterator.hasNext()) {
            Document document = iterator.next();
            return doConvert(document);
        }
        return null;
    }

    private Record doConvert(Document document) {
        switch (configuration.getDataSet().getMode()) {
        case TEXT:
            return toRecordWithWSingleDocumentContentColumn(document);
        // case MAPPING:
        // return toFlatRecordWithMapping(document);
        case JSON:
        default:
            return convertDocument2RecordDirectly(document);
        }
    }

    private Record convertDocument2RecordDirectly(Document document) {
        return documentToRecord.toRecord(document);
    }

    private Record toRecordWithWSingleDocumentContentColumn(Document document) {
        Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);

        String singleColumnName = configuration.getDataSet().getCollection();
        Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        entryBuilder.withNullable(true).withName(singleColumnName).withType(Schema.Type.STRING);
        Schema.Entry singleEntry = entryBuilder.build();
        schemaBuilder.withEntry(singleEntry);

        Schema schemaWithSingleColumn = schemaBuilder.build();

        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schemaWithSingleColumn);
        addColumn(recordBuilder, singleEntry, document);
        return recordBuilder.build();
    }

    @PreDestroy
    public void release() {
        service.closeClient(client);
    }

    private void addColumn(Record.Builder recordBuilder, final Schema.Entry entry, Object value) {
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

    private String document2Json(Document document) {
        // http://mongodb.github.io/mongo-java-driver/3.12/bson/extended-json/
        // https://github.com/mongodb/specifications/blob/master/source/extended-json.rst
        // http://mongodb.github.io/mongo-java-driver/3.12/bson/documents/
        return document.toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build(), new DocumentCodec());
    }

    private Record createRecord(Schema schema, Document document) {
        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
        schema.getEntries().forEach(entry -> addColumn(recordBuilder, entry, getValue(entry.getName(), document)));
        return recordBuilder.build();
    }

    private Object getValue(String currentName, Document document) {
        if (document == null) {
            return null;
        }
        return document.get(currentName);
    }

    private DocDBConnectionService getConnService() {
        if (conn != null) {
            return conn;
        }
        return new DocDBConnectionService();
    }
}