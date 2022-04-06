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
package org.talend.components.common.stream.input.avro;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.common.stream.AvroHelper;
import org.talend.components.common.stream.output.avro.RecordToAvro;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.beam.spi.AvroRecordBuilderFactoryProvider;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import lombok.RequiredArgsConstructor;

class AvroToRecordTest {

    private RecordBuilderFactory recordBuilderFactory;

    private GenericRecord avro;

    @BeforeEach
    protected void setUp() throws Exception {
        recordBuilderFactory = new RecordBuilderFactoryImpl("test");

        final org.apache.avro.Schema schema = SchemaBuilder
                .builder()
                .record("sample")
                .fields() //
                .name("string")
                .type()
                .stringType()
                .noDefault() //
                .name("int")
                .type()
                .intType()
                .noDefault() //
                .name("long")
                .type()
                .longType()
                .noDefault() //
                .name("double")
                .type()
                .doubleType()
                .noDefault() //
                .name("boolean")
                .type()
                .booleanType()
                .noDefault() //
                .name("array")
                .type()
                .array()
                .items()
                .intType()
                .noDefault() // Array of int
                .name("object")
                .type()
                .record("obj") // sub obj
                .fields()
                .name("f1")
                .type()
                .intType()
                .noDefault() //
                .name("f2")
                .type()
                .stringType()
                .noDefault()
                .endRecord()
                .noDefault()
                .endRecord();

        avro = new GenericData.Record(schema);
        avro.put("string", "a string sample");
        avro.put("int", 710);
        avro.put("long", 710L);
        avro.put("double", 71.0);
        avro.put("boolean", true);
        avro.put("array", Arrays.asList(1, 2, 3));

        final org.apache.avro.Schema schema1 = schema.getField("object").schema();
        GenericRecord avroObject = new GenericData.Record(schema1);
        avroObject.put("f1", 12);
        avroObject.put("f2", "Hello");
        avro.put("object", avroObject);
    }

    @Test
    void inferSchema() {
        AvroToRecord toRecord = new AvroToRecord(recordBuilderFactory);
        Schema s = toRecord.inferSchema(avro);
        assertNotNull(s);
        assertEquals(7, s.getEntries().size());
        assertTrue(s.getType().equals(Schema.Type.RECORD));
        assertTrue(s.getEntries()
                .stream()
                .map(Entry::getName)
                .collect(toList())
                .containsAll(
                        Stream.of("string", "int", "long", "double", "boolean", "array", "object").collect(toList())));
    }

    @Test
    void toRecord() {
        AvroToRecord toRecord = new AvroToRecord(recordBuilderFactory);
        Record record = toRecord.toRecord(avro);
        assertNotNull(record);
        assertEquals("a string sample", record.getString("string"));
        assertEquals(710, record.getInt("int"));
        assertEquals(710L, record.getLong("long"));
        assertEquals(71.0, record.getDouble("double"));
        assertEquals(true, record.getBoolean("boolean"));

        final Collection<Integer> integers = record.getArray(Integer.class, "array");
        assertEquals(3, integers.size());
        assertTrue(integers.contains(1));
        assertTrue(integers.contains(2));
        assertTrue(integers.contains(3));

        final Record record1 = record.getRecord("object");
        assertEquals(12, record1.getInt("f1"));
        assertEquals("Hello", record1.getString("f2"));
    }

    @Test
    void toDecimalRecord() {
        recordBuilderFactory = new RecordBuilderFactoryImpl("test");
        final org.apache.avro.Schema schema = SchemaBuilder
                .builder()
                .record("sample")
                .fields() //
                .name("decimal")
                .type(LogicalTypes.decimal(5, 2)
                        .addToSchema(org.apache.avro.Schema.createFixed("decimal", null, null, 16)))
                .noDefault() //
                .name("decimalArray")
                .type(org.apache.avro.Schema.createArray(LogicalTypes.decimal(10, 3)
                        .addToSchema(
                                org.apache.avro.Schema.createFixed(null, null, null, 16))))
                .noDefault()
                .endRecord();

        GenericData.Record decimalRecord = new GenericData.Record(schema);

        GenericData.Fixed fixedData1 =
                new GenericData.Fixed(schema.getField("decimal").schema(), decimalToBytes(new BigDecimal("123.45")));
        GenericData.Fixed fixedData2 = new GenericData.Fixed(
                AvroHelper.getUnionSchema(schema.getField("decimalArray").schema().getElementType()),
                decimalToBytes(new BigDecimal("1234.467")));
        GenericData.Fixed fixedData3 = new GenericData.Fixed(
                AvroHelper.getUnionSchema(schema.getField("decimalArray").schema().getElementType()),
                decimalToBytes(new BigDecimal("12345.678")));

        decimalRecord.put("decimal", fixedData1);
        decimalRecord.put("decimalArray", Arrays.asList(fixedData2, fixedData3));

        final AvroToRecord toRecord = new AvroToRecord(this.recordBuilderFactory);
        Record record = toRecord.toRecord(decimalRecord);
        assertNotNull(record);
        assertEquals("123.45", record.getString("decimal"));
        final Collection<BigDecimal> records = record.getArray(BigDecimal.class, "decimalArray");
        assertEquals(2, records.size());
        assertEquals(Arrays.asList(new BigDecimal("1234.467"), new BigDecimal("12345.678")), records);
    }

    @Test
    void propFile() throws IOException {
        try (final InputStream input =
                Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.avro")) {
            final DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<>();
            final DataFileStream<GenericRecord> fstream = new DataFileStream<>(input, userDatumReader);

            final AvroToRecord toRecord = new AvroToRecord(this.recordBuilderFactory);

            final GenericRecord record = fstream.next();
            final Record tckRecord = toRecord.toRecord(record);
            Assertions.assertNotNull(tckRecord);

            final Collection<Collection> properties = tckRecord.getArray(Collection.class, "properties");
            Assertions.assertEquals(2, properties.size());
            final Collection next = properties.iterator().next();
            Assertions.assertEquals(2, next.size());
            final Object recordObject = next.iterator().next();
            Assertions.assertTrue(recordObject instanceof Record);
            Record rec = (Record) recordObject;
            Assertions.assertEquals("v11", rec.getString("val"));
        }
    }

    @Test
    void propFileAvroRec() throws IOException {

        final AvroRecordBuilderFactoryProvider provider = new AvroRecordBuilderFactoryProvider();
        System.setProperty("talend.component.beam.record.factory.impl", "avro");
        final RecordBuilderFactory factory = provider.apply("test");
        try (final InputStream input =
                Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.avro")) {
            final DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<>();
            final DataFileStream<GenericRecord> fstream = new DataFileStream<>(input, userDatumReader);

            final AvroToRecord toRecord = new AvroToRecord(factory);

            final GenericRecord record = fstream.next();
            final Record tckRecord = toRecord.toRecord(record);
            Assertions.assertNotNull(tckRecord);

            final Collection<Collection> properties = tckRecord.getArray(Collection.class, "properties");
            Assertions.assertEquals(2, properties.size());
            final Collection next = properties.iterator().next();
            Assertions.assertEquals(2, next.size());
            final Object recordObject = next.iterator().next();
            Assertions.assertNotNull(recordObject);
            /*
             * Assertions.assertTrue(recordObject instanceof Record, recordObject.getClass().getName());
             * Record rec = (Record) recordObject;
             * Assertions.assertEquals("v11", rec.getString("val"));
             */
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "customers_orders.avro", "properties.avro" })
    void compareAvro(final String avroFile) throws IOException {
        final GenericRecord avroRecord = this.getRecord(avroFile);

        final AvroToRecord toRecord = new AvroToRecord(this.recordBuilderFactory);
        final Record tckRecord = toRecord.toRecord(avroRecord);

        final RecordToAvro toAvro = new RecordToAvro("test");
        final GenericRecord avroRecord2 = toAvro.fromRecord(tckRecord);

        final AvroToRecord toRecord2 = new AvroToRecord(this.recordBuilderFactory);
        final Record tckRecord2 = toRecord2.toRecord(avroRecord2);
        Assertions.assertNotNull(tckRecord2);
        Assertions.assertTrue(this.equalsSchema(avroRecord.getSchema(), tckRecord.getSchema()));
        Assertions.assertEquals(tckRecord.getSchema(), tckRecord2.getSchema());
    }

    private byte[] decimalToBytes(BigDecimal decimal) {
        byte fillByte = (byte) (decimal.signum() < 0 ? 0xFF : 0x00);
        byte[] unscaled = decimal.unscaledValue().toByteArray();
        byte[] bytes = new byte[16];
        int offset = bytes.length - unscaled.length;

        for (int i = 0; i < bytes.length; i += 1) {
            if (i < offset) {
                bytes[i] = fillByte;
            } else {
                bytes[i] = unscaled[i - offset];
            }
        }
        return bytes;
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

    boolean exploreRecord(final Record tckRecord) {
        return tckRecord.getSchema()
                .getEntries()
                .stream()
                .filter(
                        (Schema.Entry e) -> Schema.Type.ARRAY == e.getType()
                                && e.getElementSchema().getType() == Schema.Type.RECORD)
                .allMatch((Schema.Entry arrayField) -> {
                    final Collection<Record> array = tckRecord.getArray(Record.class, arrayField.getName());
                    return array.stream().allMatch((Record r) -> r.getSchema().equals(arrayField.getElementSchema()));
                });
    }

    boolean equalsSchema(final org.apache.avro.Schema avroSchemaInput, final Schema tckSchema) {

        final org.apache.avro.Schema avroSchema;
        if (avroSchemaInput.getType() == org.apache.avro.Schema.Type.UNION) {
            avroSchema = avroSchemaInput.getTypes()
                    .stream() //
                    .filter((org.apache.avro.Schema as) -> as.getType() != org.apache.avro.Schema.Type.NULL) //
                    .findFirst()
                    .get();
        } else {
            avroSchema = avroSchemaInput;
        }
        if (tckSchema.getType() == Schema.Type.RECORD) {
            if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
                return false;
            }
            if (tckSchema.getEntries().size() != avroSchema.getFields().size()) {
                return false;
            }
            final boolean hasError = tckSchema.getEntries().stream().map((Entry e) -> {
                final org.apache.avro.Schema.Field field = avroSchema.getField(e.getName());
                if (field == null) {
                    return false;
                }

                if (e.getType() == Schema.Type.ARRAY) {
                    if (!equalsSchema(field.schema().getElementType(), e.getElementSchema())) {
                        return false;
                    }
                }
                if (e.getType() == Schema.Type.RECORD) {
                    if (!equalsSchema(field.schema(), e.getElementSchema())) {
                        return false;
                    }
                }
                return true;
            }).anyMatch((Boolean res) -> res == false);
            return !hasError;
        }
        if (tckSchema.getType() == Schema.Type.ARRAY) {
            if (avroSchema.getType() != org.apache.avro.Schema.Type.ARRAY) {
                return false;
            }
            return equalsSchema(avroSchema.getElementType(), tckSchema.getElementSchema());
        }
        return true;
    }

    interface Result {

        boolean isOK();

        Result merge(Result r);

        default String label() {
            return "";
        }
    }

    final Result OK = new Result() {

        @Override
        public boolean isOK() {
            return true;
        }

        @Override
        public Result merge(Result r) {
            return r;
        }
    };

    @RequiredArgsConstructor
    class KO implements Result {

        final String label;

        @Override
        public boolean isOK() {
            return false;
        }

        @Override
        public Result merge(Result r) {
            if (r.isOK()) {
                return this;
            }
            return new KO(this.label + "\n" + r.label());
        }

        @Override
        public String label() {
            return label;
        }
    }
}