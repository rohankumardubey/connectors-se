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
package org.talend.components.couchbase.source.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.couchbase.client.core.deps.io.netty.handler.timeout.TimeoutException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinaryParser implements DocumentParser {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryParser.class);

    private final Schema schemaBinaryDocument;

    private final RecordBuilderFactory builderFactory;

    public BinaryParser(RecordBuilderFactory builderFactory) {
        this.builderFactory = builderFactory;
        schemaBinaryDocument = builderFactory
                .newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(builderFactory.newEntryBuilder().withName("id").withType(Schema.Type.STRING).build())
                .withEntry(builderFactory.newEntryBuilder().withName("content").withType(Schema.Type.BYTES).build())
                .build();
    }

    @Override
    public Record parse(Collection collection, String id) {
        GetResult result;
        try {
            result = collection.get(id, GetOptions.getOptions().transcoder(RawBinaryTranscoder.INSTANCE));
        } catch (TimeoutException | CouchbaseException e) {
            LOG.error(e.getMessage());
            throw new ComponentException(e.getMessage());
        }
        byte[] data = result.contentAs(byte[].class);

        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schemaBinaryDocument);
        recordBuilder.withString("id", id);
        recordBuilder.withBytes("content", data);
        return recordBuilder.build();
    }

}
