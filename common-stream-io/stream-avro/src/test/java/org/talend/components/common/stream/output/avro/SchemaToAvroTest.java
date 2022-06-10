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
package org.talend.components.common.stream.output.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.common.stream.input.avro.AvroToSchema;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class SchemaToAvroTest {

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");;

    @ParameterizedTest
    @ValueSource(strings = { "customers_orders.avro", "properties.avro" })
    void fromRecordSchema(final String avroFile) throws IOException {
        final GenericRecord avroRecord = this.getRecord(avroFile);

        final AvroToSchema toSchema = new AvroToSchema(this.factory);
        final SchemaToAvro toAvro = new SchemaToAvro("test");

        final Schema tckSchema = toSchema.inferSchema(avroRecord.getSchema());
        final org.apache.avro.Schema avroSchema = toAvro.fromRecordSchema("test", tckSchema);
        final Schema tckSchema2 = toSchema.inferSchema(avroSchema);

        Assertions.assertEquals(tckSchema, tckSchema2);
    }

    private GenericRecord getRecord(final String filePath) throws IOException {
        try (final InputStream input = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(filePath)) {
            final DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<>();
            final DataFileStream<GenericRecord> fstream = new DataFileStream<>(input, userDatumReader);
            return fstream.next();
        }
    }
}