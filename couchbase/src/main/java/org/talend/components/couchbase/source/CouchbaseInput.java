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
package org.talend.components.couchbase.source;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.components.couchbase.service.CouchbaseService;
import org.talend.components.couchbase.service.I18nMessage;
import org.talend.components.couchbase.source.parsers.DocumentParser;
import org.talend.components.couchbase.source.parsers.ParserFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.query.QueryResult;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Documentation("This component reads data from Couchbase.")
public class CouchbaseInput implements Serializable {

    private I18nMessage i18n;

    private static final transient Logger LOG = LoggerFactory.getLogger(CouchbaseInput.class);

    private final CouchbaseInputConfiguration configuration;

    private final RecordBuilderFactory builderFactory;

    private CouchbaseService service;

    private transient Schema schema;

    private Set<String> columnsSet;

    private transient Iterator<JsonObject> queryResultsIterator = null;

    private transient Collection collection;

    public static final String META_ID_FIELD = "_meta_id_";

    public CouchbaseInput(@Option("configuration") final CouchbaseInputConfiguration configuration,
            final CouchbaseService service, final RecordBuilderFactory builderFactory, final I18nMessage i18n) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        Cluster cluster = service.openConnection(configuration.getDataSet().getDatastore());
        Bucket bucket = cluster.bucket(configuration.getDataSet().getBucket());
        collection = service.openDefaultCollection(cluster, configuration.getDataSet().getBucket());
        if (configuration.isCreatePrimaryIndex()) {
            cluster.queryIndexes()
                    .createPrimaryIndex(bucket.name(),
                            CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));
        }
        columnsSet = new HashSet<>();

        if (configuration.getSelectAction() == SelectAction.ANALYTICS) {
            AnalyticsResult analyticsResult = null;
            try {
                analyticsResult = cluster.analyticsQuery(configuration.getQuery());
            } catch (CouchbaseException e) {
                LOG.error(i18n.queryResultError(e.getMessage()));
                throw new ComponentException(i18n.queryResultError(e.getMessage()));
            }
            queryResultsIterator = analyticsResult.rowsAsObject().iterator();
        } else {
            // DSL API (Statement, AsPath classes etc. was deprecated, cannot use it anymore!)
            // In most cases, a simple string statement is the best replacement.

            QueryResult n1qlResult;
            StringBuilder statementBuilder;
            switch (configuration.getSelectAction()) {
            case ALL:
                statementBuilder = new StringBuilder();
                statementBuilder.append("SELECT meta().id as `_meta_id_`, * FROM `").append(bucket.name()).append("`");
                if (!configuration.getLimit().isEmpty()) {
                    statementBuilder.append(" LIMIT ").append(configuration.getLimit().trim());
                }
                n1qlResult = cluster.query(statementBuilder.toString());
                break;
            case N1QL:
                /*
                 * should contain "meta().id as `_meta_id_`" field for non-json (binary) documents
                 */
                n1qlResult = cluster.query(configuration.getQuery());
                break;
            case ONE:
                statementBuilder = new StringBuilder();
                statementBuilder.append("SELECT meta().id as `_meta_id_`, * FROM `").append(bucket.name()).append("`");
                statementBuilder.append(" USE KEYS \"").append(configuration.getDocumentId()).append("\"");
                n1qlResult = cluster.query(statementBuilder.toString());
                break;
            default:
                throw new ComponentException("Select action: '" + configuration.getSelectAction() + "' is unsupported");
            }
            queryResultsIterator = n1qlResult.rowsAsObject().iterator();
        }
    }

    @Producer
    public Record next() {
        // loop to find first document with appropriate type (for non-json documents)
        while (queryResultsIterator.hasNext()) {
            JsonObject jsonObject = queryResultsIterator.next();
            if (configuration.getDataSet().getDocumentType() == DocumentType.JSON) {
                try {
                    return createJsonRecord(jsonObject);
                } catch (ClassCastException e) {
                    // document is a non-json, try to get next document
                }
            } else {
                String id = jsonObject.getString(META_ID_FIELD);
                if (id == null) {
                    LOG.error("Cannot find '_meta_id_' field. The query should contain 'meta().id as _meta_id_' field");
                    return null;
                }
                DocumentParser documentParser = ParserFactory
                        .createDocumentParser(configuration.getDataSet().getDocumentType(), builderFactory);
                return documentParser.parse(collection, id);
            }
        }
        return null;
    }

    private Record createJsonRecord(JsonObject jsonObject) {
        if (configuration.getSelectAction() == SelectAction.ALL
                || configuration.getSelectAction() == SelectAction.ONE) {
            // unwrap JSON (we use SELECT * to retrieve all values. Result will be wrapped with bucket name)
            // couldn't use bucket_name.*, in this case big float numbers (e.g. 1E100) are converted into BigInteger
            // with
            // many zeros at the end and cannot be converted back into float
            try {
                String id = jsonObject.getString(META_ID_FIELD);
                jsonObject = (JsonObject) jsonObject.get(configuration.getDataSet().getBucket());
                jsonObject.put(META_ID_FIELD, id);
            } catch (Exception e) {
                LOG.error(e.getMessage());
                throw e;
            }
        }

        if (columnsSet.isEmpty() && configuration.getDataSet().getSchema() != null
                && !configuration.getDataSet().getSchema().isEmpty()) {
            columnsSet.addAll(configuration.getDataSet().getSchema());
        }

        if (schema == null) {
            schema = service.getSchema(jsonObject, columnsSet);
        }

        return createRecord(schema, jsonObject);
    }

    @PreDestroy
    public void release() {
        service.closeConnection(configuration.getDataSet().getDatastore());
    }

    private Record createRecord(Schema schema, JsonObject jsonObject) {
        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
        schema.getEntries().forEach(entry -> addColumn(recordBuilder, entry, getValue(entry.getName(), jsonObject)));
        return recordBuilder.build();
    }

    private Object getValue(String currentName, JsonObject jsonObject) {
        if (jsonObject == null) {
            return null;
        }
        return jsonObject.get(currentName);
    }

    private void addColumn(Record.Builder recordBuilder, final Schema.Entry entry, Object value) {
        final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        Schema.Type type = entry.getType();
        entryBuilder.withName(entry.getName()).withNullable(true).withType(type);

        if (value == null)
            return;

        switch (type) {
        case ARRAY:
            Schema elementSchema = entry.getElementSchema();
            entryBuilder.withElementSchema(elementSchema);
            if (elementSchema.getType() == Schema.Type.RECORD) {
                List<Record> recordList = new ArrayList<>();
                // schema of the first element
                Schema currentSchema = elementSchema.getEntries().get(0).getElementSchema();
                for (int i = 0; i < ((JsonArray) value).size(); i++) {
                    JsonObject currentJsonObject = (JsonObject) ((JsonArray) value).get(i);
                    recordList.add(createRecord(currentSchema, currentJsonObject));
                }
                recordBuilder.withArray(entryBuilder.build(), recordList);
            } else {
                recordBuilder.withArray(entryBuilder.build(), ((JsonArray) value).toList());
            }
            break;
        case FLOAT:
            recordBuilder.withFloat(entryBuilder.build(), (Float) value);
            break;
        case DOUBLE:
            recordBuilder.withDouble(entryBuilder.build(), (Double) value);
            break;
        case BYTES:
            throw new ComponentException("BYTES is unsupported");
        case STRING:
            recordBuilder.withString(entryBuilder.build(), value.toString());
            break;
        case LONG:
            recordBuilder.withLong(entryBuilder.build(), (Long) value);
            break;
        case INT:
            recordBuilder.withInt(entryBuilder.build(), (Integer) value);
            break;
        case DATETIME:
            recordBuilder.withDateTime(entryBuilder.build(), (ZonedDateTime) value);
            break;
        case BOOLEAN:
            recordBuilder.withBoolean(entryBuilder.build(), (Boolean) value);
            break;
        case RECORD:
            entryBuilder.withElementSchema(entry.getElementSchema());
            recordBuilder.withRecord(entryBuilder.build(), createRecord(entry.getElementSchema(), (JsonObject) value));
            break;
        }
    }
}