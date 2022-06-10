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
package org.talend.components.couchbase.output;

import static com.couchbase.client.java.kv.MutateInSpec.upsert;

import java.io.Serializable;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.components.couchbase.service.CouchbaseService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryOptions;

import lombok.extern.slf4j.Slf4j;

@Version(1)
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "couchbase-output")
@Processor(name = "Output")
@Documentation("This component writes data to Couchbase.")
public class CouchbaseOutput implements Serializable {

    private transient Cluster cluster;

    private transient Collection collection;

    private String idFieldName;

    private final CouchbaseOutputConfiguration configuration;

    private final CouchbaseService service;

    private static final String CONTENT_FIELD_NAME = "content";

    public CouchbaseOutput(@Option("configuration") final CouchbaseOutputConfiguration configuration,
            final CouchbaseService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        cluster = service.openConnection(configuration.getDataSet().getDatastore());
        Bucket bucket = cluster.bucket(configuration.getDataSet().getBucket());
        collection = bucket.defaultCollection();
        idFieldName = configuration.getIdFieldName();
    }

    @ElementListener
    public void onNext(@Input final Record rec) {
        if (configuration.isUseN1QLQuery()) {
            Map<String, String> mappings = configuration
                    .getQueryParams()
                    .stream()
                    .collect(
                            Collectors.toMap(N1QLQueryParameter::getColumn, N1QLQueryParameter::getQueryParameterName));
            JsonObject namedParams = buildJsonObject(rec, mappings);
            try {
                cluster.query(configuration.getQuery(), QueryOptions.queryOptions().parameters(namedParams));
            } catch (CouchbaseException ex) {
                log.error("N1QL failed: {}.", ex.getMessage());
                throw new ComponentException(ex.getMessage());
            }
        } else {
            if (configuration.isPartialUpdate()) {
                updatePartiallyDocument(rec);
            } else {
                if (configuration.getDataSet().getDocumentType() == DocumentType.BINARY) {
                    collection.upsert(rec.getString(idFieldName), rec.getBytes(CONTENT_FIELD_NAME),
                            UpsertOptions.upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
                } else if (configuration.getDataSet().getDocumentType() == DocumentType.STRING) {
                    collection.upsert(rec.getString(idFieldName), rec.getString(CONTENT_FIELD_NAME),
                            UpsertOptions.upsertOptions().transcoder(RawStringTranscoder.INSTANCE));
                } else {
                    collection.upsert(rec.getString(idFieldName), buildJsonObjectWithoutId(rec));
                }
            }
        }
    }

    @PreDestroy
    public void release() {
        service.closeConnection(configuration.getDataSet().getDatastore());
    }

    private void updatePartiallyDocument(Record rec) {
        rec.getSchema()
                .getEntries()
                .stream()
                .filter(e -> !idFieldName.equals(e.getName()))
                .forEach(
                        e -> collection.mutateIn(rec.getString(idFieldName), Collections
                                .singletonList(upsert(e.getName(), jsonValueFromRecordValue(e, rec)))));
    }

    private Object jsonValueFromRecordValue(Schema.Entry entry, Record rec) {
        String entryName = entry.getName();
        Object value = rec.get(Object.class, entryName);
        if (null == value) {
            return null;
        }
        switch (entry.getType()) {
        case INT:
            return rec.getInt(entryName);
        case LONG:
            return rec.getLong(entryName);
        case BYTES:
            return Base64.getEncoder().encode(rec.getBytes(entryName));
        case FLOAT:
            return Double.parseDouble(String.valueOf(rec.getFloat(entryName)));
        case DOUBLE:
            return rec.getDouble(entryName);
        case STRING:
            return createJsonFromString(rec.getString(entryName));
        case BOOLEAN:
            return rec.getBoolean(entryName);
        case ARRAY:
            return JsonArray.from((List<?>) rec.getArray(List.class, entryName));
        case DATETIME:
            return rec.getDateTime(entryName).toString();
        case RECORD:
            return rec.getRecord(entryName);
        default:
            throw new ComponentException("Unknown Type " + entry.getType());
        }
    }

    private JsonObject buildJsonObject(Record rec, Map<String, String> mappings) {
        JsonObject jsonObject = JsonObject.create();
        rec.getSchema().getEntries().stream().forEach(entry -> {
            String property = mappings.getOrDefault(entry.getName(), entry.getName());
            Object value = jsonValueFromRecordValue(entry, rec);
            // need to save encoded byte array as a String
            if (value != null && value.getClass() == byte[].class) {
                value = new String((byte[]) value);
            }
            jsonObject.put(property, value);
        });
        return jsonObject;
    }

    /**
     * Calls {@link #buildJsonObject(Record, Map)} and then removes the KEY(idFieldName) from it
     *
     * @param rec
     * @return JsonObject
     */
    private JsonObject buildJsonObjectWithoutId(Record rec) {
        return buildJsonObject(rec, Collections.emptyMap()).removeKey(idFieldName);
    }

    private Object createJsonFromString(String str) {
        Object value = null;
        try {
            value = JsonObject.fromJson(str);
        } catch (Exception e) {
            // can't create JSON object from String - ignore exception
            // and try to create JSON array
            try {
                value = JsonArray.fromJson(str);
            } catch (Exception ex) {
                value = str;
            }
        }
        return value;
    }

}
