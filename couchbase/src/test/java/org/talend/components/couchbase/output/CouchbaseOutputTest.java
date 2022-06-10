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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.talend.components.couchbase.source.CouchbaseInput.META_ID_FIELD;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.TestData;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseOutput component")
class CouchbaseOutputTest extends CouchbaseUtilTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private List<JsonObject> retrieveDataFromDatabase(String prefix, int count) {
        List<JsonObject> resultList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject jsonObject = couchbaseCluster.bucket(BUCKET_NAME)
                    .defaultCollection()
                    .get(generateDocId(prefix, i))
                    .contentAsObject();
            jsonObject.put(META_ID_FIELD, generateDocId(prefix, i));
            resultList.add(jsonObject);
        }
        return resultList;
    }

    private void executeJob(CouchbaseOutputConfiguration configuration) {
        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("Couchbase_Output", "Couchbase://Output?" + outputConfig)
                .component("emitter", "test://emitter")
                .connections()
                .from("emitter")
                .to("Couchbase_Output")
                .build()
                .run();
    }

    @Test
    @DisplayName("Check fields from retrieved data")
    void simpleOutputTest() {
        log.info("Test start: simpleOutputTest");
        final String SIMPLE_OUTPUT_TEST_ID = "simpleOutputTest";
        List<Record> records = createRecords(new TestData(), SIMPLE_OUTPUT_TEST_ID);
        componentsHandler.setInputData(records);
        executeJob(getOutputConfiguration());

        List<JsonObject> resultList = retrieveDataFromDatabase(SIMPLE_OUTPUT_TEST_ID, 2);

        assertEquals(2, resultList.size());
        assertJsonEquals(new TestData(), resultList.get(0));
    }

    private void assertJsonEquals(TestData expected, JsonObject actual) {
        assertEquals(Integer.valueOf(expected.getColIntMin()), actual.getInt("t_int_min"));
        assertEquals(Integer.valueOf(expected.getColIntMax()), actual.getInt("t_int_max"));
        assertEquals(Long.valueOf(expected.getColLongMin()), actual.getLong("t_long_min"));
        assertEquals(Long.valueOf(expected.getColLongMax()), actual.getLong("t_long_max"));
        assertEquals(expected.getColFloatMin(), actual.getNumber("t_float_min").floatValue());
        assertEquals(expected.getColFloatMax(), actual.getNumber("t_float_max").floatValue());
        assertEquals(expected.getColDoubleMin(), actual.getDouble("t_double_min"));
        assertEquals(expected.getColDoubleMax(), actual.getDouble("t_double_max"));
        assertEquals(expected.isColBoolean(), actual.getBoolean("t_boolean"));
        assertEquals(expected.getColDateTime().toString(), actual.getString("t_datetime"));
        Assertions.assertArrayEquals(expected.getColList().toArray(), actual.getArray("t_array").toList().toArray());
    }

    private List<Record> createRecords(TestData testData, String id) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            records.add(testData.createRecord(recordBuilderFactory, generateDocId(id, i)));
        }
        return records;
    }

    @Test
    @DisplayName("Check binary document output")
    void outputBinaryTest() {
        log.info("Test start: outputBinaryTest");
        String idPrefix = "outputBinaryDocumentTest";
        String docContent = "DocumentContent";
        int docCount = 2;

        List<Record> records = new ArrayList<>();
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        for (int i = 0; i < docCount; i++) {
            Record record = recordBuilderFactory.newRecordBuilder()
                    .withString(entryBuilder.withName("id").withType(Schema.Type.STRING).build(),
                            generateDocId(idPrefix, i))
                    .withBytes(entryBuilder.withName("content").withType(Schema.Type.BYTES).build(),
                            (docContent + "_" + i).getBytes(StandardCharsets.UTF_8))
                    .build();
            records.add(record);
        }

        componentsHandler.setInputData(records);
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.getDataSet().setDocumentType(DocumentType.BINARY);
        configuration.setIdFieldName("id");
        executeJob(configuration);

        Collection collection = couchbaseCluster.bucket(BUCKET_NAME).defaultCollection();
        List<GetResult> resultList = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            GetResult result = collection.get(generateDocId(idPrefix, i),
                    GetOptions.getOptions().transcoder(RawBinaryTranscoder.INSTANCE));
            resultList.add(result);
        }

        assertEquals(2, resultList.size());
        for (int i = 0; i < docCount; i++) {
            GetResult getResult = resultList.get(i);
            byte[] data = getResult.contentAs(byte[].class);
            assertArrayEquals((docContent + "_" + i).getBytes(StandardCharsets.UTF_8), data);
        }
    }

    @Test
    @DisplayName("Check String document output")
    void outputStringDocumentTest() {
        log.info("Test start: outputStringDocumentTest");
        String idPrefix = "outputStringDocumentTest";
        String docContent = "StringDocumentContent";
        int docCount = 2;

        List<Record> records = new ArrayList<>();
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        for (int i = 0; i < docCount; i++) {
            Record record = recordBuilderFactory.newRecordBuilder()
                    .withString(entryBuilder.withName("id").withType(Schema.Type.STRING).build(),
                            generateDocId(idPrefix, i))
                    .withString(entryBuilder.withName("content").withType(Schema.Type.STRING).build(),
                            (docContent + "_" + i).toString())
                    .build();
            records.add(record);
        }

        componentsHandler.setInputData(records);
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.getDataSet().setDocumentType(DocumentType.STRING);
        configuration.setIdFieldName("id");
        executeJob(configuration);

        Collection collection = couchbaseCluster.bucket(BUCKET_NAME).defaultCollection();
        List<GetResult> resultList = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            GetResult result = collection.get(generateDocId(idPrefix, i),
                    GetOptions.getOptions().transcoder(RawStringTranscoder.INSTANCE));
            resultList.add(result);
        }

        assertEquals(2, resultList.size());
        for (int i = 0; i < docCount; i++) {
            GetResult getResult = resultList.get(i);
            String content = getResult.contentAs(String.class);
            assertEquals((docContent + "_" + i).toString(), content);
        }

    }

    @Test
    @DisplayName("Simple N1QL query with no parameters")
    void executeSimpleN1QLQueryWithNoParameters() {
        log.info("Test start: executeSimpleN1QLQueryWithNoParameters");
        final String N1QL_WITH_NO_PARAMETERS_ID_PREFIX = "n1qlNoParametersIdPrefix";
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.setUseN1QLQuery(true);

        TestData td = new TestData();
        td.setColDoubleMax(Integer.MAX_VALUE);
        td.setColFloatMax(Integer.MAX_VALUE);
        td.setColDoubleMin(Float.MIN_VALUE);
        td.setColLongMin(Integer.MIN_VALUE);

        String js = td.createJson("").toString();
        String id = generateDocId(N1QL_WITH_NO_PARAMETERS_ID_PREFIX, 0);
        String qry = String.format("UPSERT INTO `%s` (KEY, VALUE) VALUES (\"%s\", %s)", BUCKET_NAME, id, js);
        configuration.setQuery(qry);
        componentsHandler.setInputData(createRecords(new TestData(), N1QL_WITH_NO_PARAMETERS_ID_PREFIX));
        executeJob(configuration);
        List<JsonObject> resultList = retrieveDataFromDatabase(N1QL_WITH_NO_PARAMETERS_ID_PREFIX, 1);
        assertEquals(1, resultList.size());
        JsonObject result = resultList.get(0);
        assertJsonEquals(td, result);
        assertEquals(generateDocId(N1QL_WITH_NO_PARAMETERS_ID_PREFIX, 0), result.getString(META_ID_FIELD));
    }

    @Test
    @DisplayName("N1QL query with parameters")
    void executeSimpleN1QLQueryWithParameters() {
        log.info("Test start: executeSimpleN1QLQueryWithParameters");
        final String N1QL_WITH_PARAMETERS_ID_PREFIX = "n1qlWithParametersIdPrefix";
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.setUseN1QLQuery(true);
        String js = new TestData().createParameterizedJsonString();
        String qry = String.format("INSERT INTO `%s` (KEY, VALUE) VALUES ($t_string, " + js + ")", BUCKET_NAME);
        configuration.setQuery(qry);

        List<N1QLQueryParameter> params = new ArrayList<>();
        params.add(new N1QLQueryParameter("$t_string", "t_string"));
        params.add(new N1QLQueryParameter("$t_int_min", "t_int_min"));
        params.add(new N1QLQueryParameter("$t_int_max", "t_int_max"));
        params.add(new N1QLQueryParameter("$t_long_min", "t_long_min"));
        params.add(new N1QLQueryParameter("$t_long_max", "t_long_max"));
        params.add(new N1QLQueryParameter("$t_float_min", "t_float_min"));
        params.add(new N1QLQueryParameter("$t_float_max", "t_float_max"));
        params.add(new N1QLQueryParameter("$t_double_min", "t_double_min"));
        params.add(new N1QLQueryParameter("$t_double_max", "t_double_max"));
        params.add(new N1QLQueryParameter("$t_boolean", "t_boolean"));
        params.add(new N1QLQueryParameter("$t_datetime", "t_datetime"));
        params.add(new N1QLQueryParameter("$t_array", "t_array"));
        configuration.setQueryParams(params);
        TestData td = new TestData();
        td.setColDoubleMax(Integer.MAX_VALUE);
        td.setColFloatMax(Integer.MAX_VALUE);
        td.setColLongMax(Integer.MAX_VALUE);

        componentsHandler.setInputData(createRecords(td, N1QL_WITH_PARAMETERS_ID_PREFIX));
        executeJob(configuration);
        List<JsonObject> resultList = retrieveDataFromDatabase(N1QL_WITH_PARAMETERS_ID_PREFIX, 2);
        assertEquals(2, resultList.size());
        for (JsonObject json : resultList) {
            assertJsonEquals(td, json);
        }
    }

    private List<Record> createPartialUpdateRecords(String idPrefix) {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        List<Record> records = new ArrayList<>();
        Record record1 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(),
                        generateDocId(idPrefix, 0))
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), 1971)
                .withString(entryBuilder.withName("extra_content").withType(Schema.Type.STRING).build(), "path new")
                .build();
        Record record2 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(),
                        generateDocId(idPrefix, 1))
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), Boolean.FALSE)
                .withString(entryBuilder.withName("extra_content2").withType(Schema.Type.STRING).build(), "path zap")
                .build();
        records.add(record1);
        records.add(record2);

        return records;
    }

    @Test
    @DisplayName("Document partial update")
    void partialUpdate() {
        log.info("Test start: partialUpdate");
        final String PARTIAL_UPDATE_ID_PREFIX = "partialUpdate";
        // prepare data
        Bucket bucket = couchbaseCluster.bucket(BUCKET_NAME);
        Collection collection = bucket.defaultCollection();
        for (int i = 0; i < 2; i++) {
            JsonObject js = new TestData().createJson(PARTIAL_UPDATE_ID_PREFIX);
            collection.insert(generateDocId(PARTIAL_UPDATE_ID_PREFIX, i), js);
        }

        // update data
        CouchbaseOutputConfiguration config = getOutputConfiguration();
        config.setPartialUpdate(true);
        componentsHandler.setInputData(createPartialUpdateRecords(PARTIAL_UPDATE_ID_PREFIX));
        executeJob(config);
        //
        List<JsonObject> resultList = retrieveDataFromDatabase(PARTIAL_UPDATE_ID_PREFIX, 2);
        assertEquals(2, resultList.size());
        TestData testData = new TestData();
        Stream.iterate(0, o -> o + 1).limit(2).forEach(idx -> {
            // untouched properties
            assertEquals(Integer.valueOf(testData.getColIntMax()), resultList.get(idx).getInt("t_int_max"));
            assertEquals(Long.valueOf(testData.getColLongMin()), resultList.get(idx).getLong("t_long_min"));
            assertEquals(Long.valueOf(testData.getColLongMax()), resultList.get(idx).getLong("t_long_max"));
            assertEquals(testData.getColFloatMin(),
                    resultList.get(idx).getNumber("t_float_min").floatValue());
            assertEquals(testData.getColFloatMax(),
                    resultList.get(idx).getNumber("t_float_max").floatValue());
            assertEquals(testData.getColDoubleMin(), resultList.get(idx).getDouble("t_double_min"));
            assertEquals(testData.getColDoubleMax(), resultList.get(idx).getDouble("t_double_max"));
            assertEquals(testData.getColDateTime().toString(), resultList.get(idx).getString("t_datetime"));
            assertArrayEquals(testData.getColList().toArray(),
                    resultList.get(idx).getArray("t_array").toList().toArray());
            // upserted proterties
            if (idx == 0) {
                assertEquals(1971, resultList.get(idx).getInt("t_int_min"));
                assertEquals(testData.isColBoolean(), resultList.get(idx).getBoolean("t_boolean"));
                assertEquals("path new", resultList.get(idx).getString("extra_content"));
                assertNull(resultList.get(idx).getString("extra_content2"));
            } else {
                assertEquals(Integer.valueOf(testData.getColIntMin()),
                        resultList.get(idx).getInt("t_int_min"));
                assertEquals(Boolean.FALSE, resultList.get(idx).getBoolean("t_boolean"));
                assertEquals("path zap", resultList.get(idx).getString("extra_content2"));
                assertNull(resultList.get(idx).getString("extra_content"));
            }
        });
    }

    private CouchbaseOutputConfiguration getOutputConfiguration() {
        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setBucket(BUCKET_NAME);
        couchbaseDataSet.setDatastore(couchbaseDataStore);

        CouchbaseOutputConfiguration configuration = new CouchbaseOutputConfiguration();
        configuration.setIdFieldName("t_string");
        configuration.setDataSet(couchbaseDataSet);
        return configuration;
    }

    @Test
    @DisplayName("Document with encoded string")
    void toJsonDocumentWithBytesType() {
        byte[] bytes = "aloha".getBytes(Charset.defaultCharset());
        String encoded = new String(Base64.getEncoder().encode(bytes));
        String idValue = "fixBytes";
        Record test = recordBuilderFactory.newRecordBuilder()
                .withString("ID", idValue)
                .withInt("id", 101)
                .withString("name", "kamikaze")
                .withString("byties", encoded)
                .build();
        JsonObject jsonObject = JsonObject.fromJson(test.toString());
        assertEquals(idValue, jsonObject.getString("ID"));
        assertEquals(101, jsonObject.getInt("id"));
        assertEquals("kamikaze", jsonObject.getString("name"));
        byte[] rbytes = Base64.getDecoder().decode(jsonObject.getString("byties"));
        assertEquals(bytes.length, rbytes.length);
        assertEquals("aloha", new String(rbytes, Charset.defaultCharset()));
    }

}
