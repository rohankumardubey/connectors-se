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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;

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
    public Record parse(Bucket bucket, String id) {
        BinaryDocument doc;
        try {
            doc = bucket.get(id, BinaryDocument.class);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
        byte[] data = new byte[doc.content().readableBytes()];
        doc.content().readBytes(data);
        ReferenceCountUtil.release(doc.content());

        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schemaBinaryDocument);
        recordBuilder.withString("id", id);
        recordBuilder.withBytes("content", data);
        return recordBuilder.build();
    }

}
