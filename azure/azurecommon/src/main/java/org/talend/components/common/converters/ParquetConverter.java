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
package org.talend.components.common.converters;

import org.apache.avro.generic.GenericRecord;
import org.talend.components.common.formats.ParquetFormatOptions;
import org.talend.components.common.stream.input.avro.AvroToRecord;
import org.talend.components.common.stream.output.avro.RecordToAvro;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ParquetConverter implements RecordConverter<GenericRecord> {

    protected static final String DEFAULT_RECORD_NAMESPACE = "org.talend.components";

    private AvroToRecord avroToRecord;

    private RecordToAvro recordToAvro;

    @Override
    public Schema inferSchema(GenericRecord record) {
        return avroToRecord.inferSchema(record);
    }

    @Override
    public Record toRecord(GenericRecord record) {
        return avroToRecord.toRecord(record);
    }

    @Override
    public GenericRecord fromRecord(Record record) {
        return recordToAvro.fromRecord(record);
    }

    public static ParquetConverter of(RecordBuilderFactory recordBuilderFactory) {
        return new ParquetConverter(recordBuilderFactory, DEFAULT_RECORD_NAMESPACE);
    }

    public static ParquetConverter of(RecordBuilderFactory recordBuilderFactory, ParquetFormatOptions config,
            String recordNameSpace) {
        return new ParquetConverter(recordBuilderFactory, recordNameSpace);
    }

    private ParquetConverter(RecordBuilderFactory recordBuilderFactory, String recordNameSpace) {
        this(recordBuilderFactory, null, recordNameSpace);
    }

    protected ParquetConverter(RecordBuilderFactory factory, ParquetFormatOptions config,
            String currentRecordNamespace) {
        this.avroToRecord = new AvroToRecord(factory);
        this.recordToAvro =
                new RecordToAvro(currentRecordNamespace == null ? DEFAULT_RECORD_NAMESPACE : currentRecordNamespace);
    }

    public org.apache.avro.Schema inferAvroSchema(Schema schema) {
        return recordToAvro.fromRecordSchema(schema);
    }
}
