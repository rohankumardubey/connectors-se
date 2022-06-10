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
package org.talend.components.common.stream;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.components.common.stream.input.avro.AvroReaderSupplier;
import org.talend.components.common.stream.output.avro.AvroWriterSupplier;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.beam.spi.AvroRecordBuilderFactoryProvider;

@WithComponents("org.talend.components.common.stream.api")
class AvroFormatTest {

    @Service
    private RecordIORepository repo;

    @Test
    void format() {
        final RecordReaderSupplier reader = repo.findReader(AvroConfiguration.class);
        Assertions.assertNotNull(reader);
        Assertions.assertTrue(AvroReaderSupplier.class.isInstance(reader));

        final RecordWriterSupplier writer = repo.findWriter(AvroConfiguration.class);
        Assertions.assertNotNull(writer);
        Assertions.assertTrue(AvroWriterSupplier.class.isInstance(writer));
    }

    @Test
    void avroTest() {
        // get RecordBuilderFactory
        AvroRecordBuilderFactoryProvider recordBuilderFactoryProvider = new AvroRecordBuilderFactoryProvider();
        System.setProperty("talend.component.beam.record.factory.impl", "avro");
        RecordBuilderFactory recordBuilderFactory = recordBuilderFactoryProvider.apply("test");
        // customer record schema
        org.talend.sdk.component.api.record.Schema.Builder schemaBuilder = recordBuilderFactory
                .newSchemaBuilder(Schema.Type.RECORD);
        Schema.Entry nameEntry = recordBuilderFactory.newEntryBuilder()
                .withName("name")
                .withNullable(true)
                .withType(Schema.Type.STRING)
                .build();
        Schema.Entry ageEntry = recordBuilderFactory.newEntryBuilder()
                .withName("age")
                .withNullable(true)
                .withType(Schema.Type.INT)
                .build();
        Schema customerSchema = schemaBuilder.withEntry(nameEntry).withEntry(ageEntry).build();
        // record 1
        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(customerSchema);
        recordBuilder.withString("name", "Tom Cruise");
        recordBuilder.withInt("age", 58);
        Record record1 = recordBuilder.build();
        // record 2
        recordBuilder = recordBuilderFactory.newRecordBuilder(customerSchema);
        recordBuilder.withString("name", "Meryl Streep");
        recordBuilder.withInt("age", 63);
        Record record2 = recordBuilder.build();
        // list 1
        Collection<Record> list1 = new ArrayList<>();
        list1.add(record1);
        list1.add(record2);
        // record 3
        recordBuilder = recordBuilderFactory.newRecordBuilder(customerSchema);
        recordBuilder.withString("name", "Client Eastwood");
        recordBuilder.withInt("age", 89);
        Record record3 = recordBuilder.build();
        // record 4
        recordBuilder = recordBuilderFactory.newRecordBuilder(customerSchema);
        recordBuilder.withString("name", "Jessica Chastain");
        recordBuilder.withInt("age", 36);
        Record record4 = recordBuilder.build();
        // list 2
        Collection<Record> list2 = new ArrayList<Record>();
        list2.add(record3);
        list2.add(record4);
        // main list
        Collection<Object> list3 = new ArrayList<Object>();
        list3.add(list1);
        list3.add(list2);
        // schema of sub list
        schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.ARRAY);
        Schema subListSchema = schemaBuilder.withElementSchema(customerSchema).build();
        // main record
        recordBuilder = recordBuilderFactory.newRecordBuilder();
        Schema.Entry entry = recordBuilderFactory.newEntryBuilder()
                .withName("customers")
                .withNullable(true)
                .withType(Schema.Type.ARRAY)
                .withElementSchema(subListSchema)
                .build();
        recordBuilder.withArray(entry, list3);
        Record record = recordBuilder.build();
        Assertions.assertNotNull(record);
    }
}